package sched

import (
	"time"

	"github.com/neo1202/rudp-scheduler/rudp"
	"github.com/neo1202/rudp-scheduler/wal"
	"github.com/neo1202/rudp-scheduler/wire"
	"github.com/neo1202/rudp-scheduler/workload"
)

// housekeeping is how often the aggregator syncs the log and looks for jobs
// and answers that have outlived their welcome.
const housekeeping = 50 * time.Millisecond

// compactAbove is the log size beyond which an idle aggregator rewrites it.
const compactAbove = 1 << 20

// finished is a completed job's answer, kept for clients that ask again.
type finished struct {
	result workload.Partial
	at     time.Time
}

// aggregator owns done, jobs, the stored answers and the write-ahead log.
// De-duplicating a result, merging it, counting down, logging it, and
// answering the client once the last one is in are one critical section;
// putting them all in one goroutine is what makes a lock unnecessary.
//
// This is where end-to-end exactly-once is established. The transport below
// delivers at least once, in any order. done absorbs repetition (transport
// duplicates that slipped past a workerLoop, late copies of speculated tasks,
// recomputation after a worker died or the server restarted); the workload's
// Merge is commutative and associative, which absorbs reordering. The
// aggregator only ever sees the job through the Workload interface.
func (s *Scheduler) aggregator(boot *recovered) {
	var (
		done    = boot.done // only the aggregator touches these
		jobs    = boot.jobs
		results = boot.results
		byConn  = map[int]uint64{} // client connection -> the job it is waiting for
		log     = boot.log
		dirty   bool
		stats   = boot.stats
	)
	tick := time.NewTicker(housekeeping)
	defer tick.Stop()
	if log != nil {
		defer log.Close()
	}

	record := func(r wal.Record, syncNow bool) {
		if log == nil {
			return
		}
		err := log.Append(r)
		if err == nil && syncNow {
			err, dirty = log.Sync(), false
		} else {
			dirty = true
		}
		if err != nil {
			stats.WALErrors++
		}
	}
	reply := func(h *rudp.ConnHandle, id uint64, p workload.Partial) {
		if h != nil {
			h.Write(wire.Encode(wire.Result{Job: id, Hash: p.Hash, Nonce: p.Nonce}))
		}
	}
	detach := func(j *job) {
		if j.client != nil {
			delete(byConn, j.client.ID())
			j.client = nil
		}
	}
	finish := func(j *job, completed bool) {
		id := j.spec.id
		detach(j)
		delete(jobs, id)
		for i := uint32(0); i < j.spec.n; i++ { // and the done entries under it
			delete(done, wire.TaskID{Job: id, Idx: i})
		}
		close(j.cancel)
		if completed {
			results[id] = finished{j.acc, time.Now()}
			record(wal.JobDone{Job: id, Hash: j.acc.Hash, Nonce: j.acc.Nonce}, true)
			stats.JobsCompleted++
			stats.JobLatency.observe(time.Since(j.started).Seconds())
		} else {
			record(wal.JobDrop{Job: id}, false)
			stats.JobsDropped++
		}
		if s.p.Hooks != nil && s.p.Hooks.OnJobEnd != nil {
			s.p.Hooks.OnJobEnd(id, completed)
		}
	}

	for {
		select {
		case req := <-s.submitCh:
			id := req.spec.id
			if f, ok := results[id]; ok { // already answered once: answer again
				reply(req.h, id, f.result)
				stats.Reattached++
				req.reply <- submitReply{}
				break
			}
			j, running := jobs[id]
			if running { // the client came back, or the job was recovered from the log
				detach(j)
				stats.Reattached++
			} else {
				j = &job{spec: req.spec, remaining: req.spec.n, acc: s.wl.Zero(), started: time.Now(), cancel: make(chan struct{})}
				jobs[id] = j
				record(wal.JobStart{Job: id, Lo: req.spec.lo, Hi: req.spec.hi, Size: req.spec.size, N: req.spec.n, Msg: req.spec.msg}, true)
				stats.JobsStarted++
			}
			select {
			case <-req.h.Done():
				// The client vanished before its handler got here, and the
				// dispatcher's notice may already have come and gone.
				j.orphanedAt = time.Now()
			default:
				j.client, j.orphanedAt = req.h, time.Time{}
				byConn[req.h.ID()] = id
			}
			req.reply <- submitReply{feed: !running, cancel: j.cancel}
			if j.remaining == 0 { // empty range: nothing to wait for
				reply(j.client, id, j.acc)
				finish(j, true)
			}

		case r := <-s.resultsCh:
			j, ok := jobs[r.ID.Job]
			if !ok || r.ID.Idx >= j.spec.n || done[r.ID] {
				// The job is gone, or this is a late copy: drop it.
				dup := ok && r.ID.Idx < j.spec.n
				if dup {
					stats.Duplicates++
				} else {
					stats.Orphans++
				}
				if s.p.Hooks != nil && s.p.Hooks.OnDiscard != nil {
					s.p.Hooks.OnDiscard(r.ID, dup)
				}
				break
			}
			done[r.ID] = true
			j.acc = s.wl.Merge(j.acc, workload.Partial{Hash: r.Hash, Nonce: r.Nonce})
			j.remaining--
			stats.Merged++
			// Not synced here. If a crash loses this record the chunk is
			// computed and merged again, which changes nothing.
			record(wal.ChunkDone{Job: r.ID.Job, Idx: r.ID.Idx, Hash: r.Hash, Nonce: r.Nonce}, false)
			if s.p.Hooks != nil && s.p.Hooks.OnMerge != nil {
				s.p.Hooks.OnMerge(r.ID)
			}
			if j.remaining == 0 {
				reply(j.client, r.ID.Job, j.acc)
				finish(j, true)
			}

		case conn := <-s.clientGoneCh:
			// The job lives on for a grace period: the client may only have
			// been declared lost by a run of dropped heartbeats.
			if j, ok := jobs[byConn[conn]]; ok && j.client != nil && j.client.ID() == conn {
				detach(j)
				j.orphanedAt = time.Now()
			}
			delete(byConn, conn)

		case now := <-tick.C:
			grace := time.Duration(s.p.JobGraceMs) * time.Millisecond
			for _, j := range jobs {
				if !j.orphanedAt.IsZero() && now.Sub(j.orphanedAt) > grace {
					finish(j, false)
				}
			}
			ttl := time.Duration(s.p.ResultTTLMs) * time.Millisecond
			for id, f := range results {
				if now.Sub(f.at) > ttl {
					delete(results, id)
					record(wal.JobDrop{Job: id}, false)
				}
			}
			if log != nil && len(jobs) == 0 && log.Size() > compactAbove {
				if err := log.Rewrite(snapshotResults(results)); err != nil {
					stats.WALErrors++
				}
				dirty = false
			}
			if log != nil && dirty {
				if err := log.Sync(); err != nil {
					stats.WALErrors++
				}
				dirty = false
			}

		case <-s.stop:
			return
		}

		stats.ActiveJobs = len(jobs)
		if s.p.AggStats != nil {
			select {
			case s.p.AggStats <- stats:
			default:
			}
		}
	}
}
