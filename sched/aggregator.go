package sched

import (
	"time"

	"github.com/neo1202/rudp-scheduler/wire"
	"github.com/neo1202/rudp-scheduler/workload"
)

// aggregator owns done and jobs. De-duplicating a result, merging it, counting
// down, and answering the client once the last one is in are one critical
// section; putting all four in one goroutine is what makes a lock unnecessary.
//
// This is where end-to-end exactly-once is established. The transport below
// delivers at least once, in any order. done absorbs repetition (transport
// duplicates that slipped past a workerLoop, late copies of speculated tasks,
// recomputation after a worker died); the workload's Merge is commutative and
// associative, which absorbs reordering. The aggregator only ever sees the
// job through the Workload interface.
func (s *Scheduler) aggregator() {
	done := map[wire.TaskID]bool{} // only the aggregator touches these
	jobs := map[uint32]*job{}
	var stats AggStats

	finish := func(client uint32, j *job, completed bool) {
		delete(jobs, client)
		for i := uint32(0); i < j.n; i++ { // and the done entries under it
			delete(done, wire.TaskID{Client: client, Idx: i})
		}
		if completed {
			stats.JobsCompleted++
			stats.JobLatency.observe(time.Since(j.started).Seconds())
		} else {
			stats.JobsDropped++
		}
		if s.p.Hooks != nil && s.p.Hooks.OnJobEnd != nil {
			s.p.Hooks.OnJobEnd(int(client), completed)
		}
	}
	reply := func(j *job) {
		j.client.Write(wire.Encode(wire.Result{Hash: j.acc.Hash, Nonce: j.acc.Nonce}))
	}

	for {
		select {
		case j := <-s.newJobCh:
			client := uint32(j.client.ID())
			select {
			case <-j.client.Done():
				// The client vanished before its handler got here, and the
				// dispatcher's notice may already have come and gone.
				continue
			default:
			}
			stats.JobsStarted++
			jobs[client] = j
			if j.remaining == 0 { // empty range: nothing to wait for
				reply(j)
				finish(client, j, true)
			}

		case r := <-s.resultsCh:
			j, ok := jobs[r.ID.Client]
			if !ok || r.ID.Idx >= j.n || done[r.ID] {
				// The client has left, or this is a late copy: drop it.
				dup := ok && r.ID.Idx < j.n
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
			if s.p.Hooks != nil && s.p.Hooks.OnMerge != nil {
				s.p.Hooks.OnMerge(r.ID)
			}
			if j.remaining == 0 {
				reply(j)
				finish(r.ID.Client, j, true)
			}

		case id := <-s.clientGoneCh:
			if j, ok := jobs[uint32(id)]; ok {
				finish(uint32(id), j, false)
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
