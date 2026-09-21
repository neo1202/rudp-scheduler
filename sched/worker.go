package sched

import (
	"time"

	"github.com/neo1202/rudp-scheduler/rudp"
	"github.com/neo1202/rudp-scheduler/wire"
)

// entry is one task in flight at a worker.
type entry struct {
	task       Task
	sentAt     time.Time
	speculated bool // a clone has been pushed to pq; at most once per entry
}

// worker is the server-side proxy of one remote worker. The channels are how
// the dispatcher reaches it; everything else belongs to its workerLoop.
type worker struct {
	h        *rudp.ConnHandle
	resultCh chan wire.ChunkResult // dispatcher -> workerLoop, buffered
	dead     chan struct{}         // closed by the dispatcher when the connection is lost

	// owned by workerLoop
	inflight map[wire.TaskID]*entry // len <= window
	srtt     time.Duration          // task-level smoothed round trip
	hasSRTT  bool
	pqHold   bool // skip pq until the next tick (we just pulled our own clone)

	stats       WorkerStats
	joined      time.Time
	lastAccount time.Time
}

func newWorker(h *rudp.ConnHandle, window int) *worker {
	now := time.Now()
	return &worker{
		h:           h,
		resultCh:    make(chan wire.ChunkResult, 4*window),
		dead:        make(chan struct{}),
		inflight:    make(map[wire.TaskID]*entry),
		stats:       WorkerStats{Worker: h.ID()},
		joined:      now,
		lastAccount: now,
	}
}

// workerLoop is the whole load balancer. Nobody assigns work: a worker pulls a
// task whenever it has room in its window, so fast workers simply come back
// more often. Setting pqCh and nqCh to nil closes those select cases while the
// window is full.
//
// It also finds its own stragglers. inflight is private to this goroutine, so
// the scan needs no lock and no other goroutine ever reads this map.
func (s *Scheduler) workerLoop(w *worker) {
	tick := time.NewTicker(time.Duration(s.p.TickMs) * time.Millisecond)
	defer tick.Stop()
	for {
		var pqCh, nqCh <-chan Task // nil: do not pull
		if len(w.inflight) < s.window {
			if !w.pqHold {
				// Look at pq first, without waiting. A plain two-way select
				// picks at random when both are ready, which would let urgent
				// re-issues queue behind ordinary tasks half the time.
				select {
				case t := <-s.pq:
					s.send(w, t)
					continue
				default:
				}
				pqCh = s.pq
			}
			nqCh = s.nq
		}

		select {
		case r := <-w.resultCh: // forwarded by the dispatcher
			e, ok := w.inflight[r.ID]
			if !ok {
				w.stats.DupResults++ // delivered twice by the transport: already handled
				continue
			}
			w.account()
			delete(w.inflight, r.ID)
			w.stats.Results++
			if !e.speculated {
				// Same idea as Karn's rule one layer down: a task we already
				// gave up on says nothing about this worker's normal pace.
				w.updateSRTT(time.Since(e.sentAt))
			}
			select {
			case s.resultsCh <- r: // de-duplication and merging are the aggregator's business
			case <-s.stop:
				return
			}

		case t := <-pqCh:
			s.send(w, t)

		case t := <-nqCh:
			s.send(w, t)

		case <-tick.C:
			w.pqHold = false
			s.speculate(w)
			s.publishWorker(w, false)

		case <-w.dead: // the remote worker is gone: hand everything back
			w.account()
			for id, e := range w.inflight {
				select {
				case s.pq <- e.task: // may be recomputed; the aggregator de-duplicates
					w.stats.Requeued++
				case <-s.stop:
					return
				}
				delete(w.inflight, id)
			}
			s.publishWorker(w, true)
			return

		case <-s.stop:
			return
		}
	}
}

// send records the task as in flight and writes it to the worker. Write never
// blocks, and because len(inflight) < window was checked first, the message
// goes straight into the transport's window.
func (s *Scheduler) send(w *worker, t Task) {
	if _, mine := w.inflight[t.ID]; mine {
		// This is our own clone coming back from pq. The original is still in
		// flight here, so put the clone back for somebody else and leave pq
		// alone until the next tick. If pq is full the clone is dropped: the
		// task is not lost, the original is still ours.
		select {
		case s.pq <- t:
		default:
		}
		w.pqHold = true
		return
	}
	w.account()
	w.inflight[t.ID] = &entry{task: t, sentAt: time.Now()}
	w.h.Write(wire.Encode(wire.Chunk{ID: t.ID, Lo: t.Lo, Hi: t.Hi, Msg: t.Msg}))
	w.stats.Sent++
	if s.p.Hooks != nil && s.p.Hooks.OnSend != nil {
		s.p.Hooks.OnSend(w.h.ID(), t.ID, len(w.inflight))
	}
}

// speculate clones every task that has been out for too long into pq. The
// push is non-blocking: if pq is full the entry stays unmarked and is retried
// on the next tick, so a live worker never waits on pq.
func (s *Scheduler) speculate(w *worker) {
	if s.p.NoSpeculate || !w.hasSRTT {
		return // no sample yet: nothing to compare against
	}
	threshold := time.Duration(s.p.StragglerFactor * float64(w.srtt))
	if floor := time.Duration(s.p.MinStragglerMs) * time.Millisecond; threshold < floor {
		threshold = floor
	}
	now := time.Now()
	for id, e := range w.inflight {
		if e.speculated || now.Sub(e.sentAt) <= threshold {
			continue
		}
		select {
		case s.pq <- e.task:
			e.speculated = true
			w.stats.Speculated++
			if s.p.Hooks != nil && s.p.Hooks.OnSpeculate != nil {
				s.p.Hooks.OnSpeculate(w.h.ID(), id)
			}
		default:
		}
	}
}

func (w *worker) updateSRTT(sample time.Duration) {
	if !w.hasSRTT {
		w.srtt, w.hasSRTT = sample, true
		return
	}
	w.srtt = (7*w.srtt + sample) / 8
}

// account charges the time since the last call to Busy if any task was in
// flight during it. Call it before every change to inflight.
func (w *worker) account() {
	now := time.Now()
	if len(w.inflight) > 0 {
		w.stats.Busy += now.Sub(w.lastAccount)
	}
	w.lastAccount = now
}

func (s *Scheduler) publishWorker(w *worker, gone bool) {
	if s.p.WorkerStats == nil {
		return
	}
	w.account()
	st := w.stats
	st.Inflight = len(w.inflight)
	st.SRTT = w.srtt
	st.Alive = time.Since(w.joined)
	st.Gone = gone
	select {
	case s.p.WorkerStats <- st:
	default:
	}
}
