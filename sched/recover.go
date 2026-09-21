package sched

import (
	"time"

	"github.com/neo1202/rudp-scheduler/wal"
	"github.com/neo1202/rudp-scheduler/wire"
	"github.com/neo1202/rudp-scheduler/workload"
)

// recovered is the aggregator's state as rebuilt from the write-ahead log. It
// is assembled before any goroutine runs and then handed to the aggregator,
// so it never has two owners.
type recovered struct {
	log     *wal.Log
	done    map[wire.TaskID]bool
	jobs    map[uint64]*job
	results map[uint64]finished
	feeds   []resume
	stats   AggStats
}

// resume describes one unfinished job whose remaining tasks must be queued
// again. skip is a private copy for the feeder goroutine.
type resume struct {
	spec   jobSpec
	skip   map[uint32]bool
	cancel <-chan struct{}
}

// recover replays the log. Replaying is the same fold the aggregator performs
// live: de-duplicate by TaskID, Merge, count down. Records may repeat (a chunk
// counted twice across a crash) and that is harmless for the same reason
// duplicates are harmless on the wire.
func (s *Scheduler) recover() (*recovered, error) {
	r := &recovered{
		done:    map[wire.TaskID]bool{},
		jobs:    map[uint64]*job{},
		results: map[uint64]finished{},
	}
	if s.p.WALPath == "" {
		return r, nil
	}
	log, recs, err := wal.Open(s.p.WALPath)
	if err != nil {
		return nil, err
	}
	r.log = log
	now := time.Now()

	chunks := map[uint64][]wal.ChunkDone{} // kept so the compacted log can repeat them
	for _, rec := range recs {
		switch rec := rec.(type) {
		case wal.JobStart:
			if _, dup := r.jobs[rec.Job]; dup {
				continue
			}
			delete(r.results, rec.Job)
			r.jobs[rec.Job] = &job{
				spec:       jobSpec{id: rec.Job, lo: rec.Lo, hi: rec.Hi, size: rec.Size, n: rec.N, msg: rec.Msg},
				remaining:  rec.N,
				acc:        s.wl.Zero(),
				started:    now,
				orphanedAt: now, // nobody is attached yet; the grace period starts now
				cancel:     make(chan struct{}),
			}
		case wal.ChunkDone:
			id := wire.TaskID{Job: rec.Job, Idx: rec.Idx}
			j, ok := r.jobs[rec.Job]
			if !ok || rec.Idx >= j.spec.n || r.done[id] {
				continue
			}
			r.done[id] = true
			j.acc = s.wl.Merge(j.acc, workload.Partial{Hash: rec.Hash, Nonce: rec.Nonce})
			j.remaining--
			chunks[rec.Job] = append(chunks[rec.Job], rec)
		case wal.JobDone:
			r.forget(rec.Job)
			r.results[rec.Job] = finished{workload.Partial{Hash: rec.Hash, Nonce: rec.Nonce}, now}
		case wal.JobDrop:
			r.forget(rec.Job)
			delete(r.results, rec.Job)
		}
	}

	// A job whose last chunk was logged but whose JobDone was not is finished.
	for id, j := range r.jobs {
		if j.remaining == 0 {
			r.forget(id)
			r.results[id] = finished{j.acc, now}
		}
	}

	// Compact: rewrite the log as the shortest history that yields this state.
	compact := snapshotResults(r.results)
	for id, j := range r.jobs {
		compact = append(compact, wal.JobStart{Job: id, Lo: j.spec.lo, Hi: j.spec.hi, Size: j.spec.size, N: j.spec.n, Msg: j.spec.msg})
		skip := map[uint32]bool{}
		for _, c := range chunks[id] {
			compact = append(compact, c)
			skip[c.Idx] = true
		}
		r.feeds = append(r.feeds, resume{spec: j.spec, skip: skip, cancel: j.cancel})
		r.stats.JobsRecovered++
		r.stats.ChunksRecovered += uint64(len(skip))
	}
	if len(recs) > 0 {
		if err := log.Rewrite(compact); err != nil {
			log.Close()
			return nil, err
		}
	}
	return r, nil
}

// forget removes a running job and its done entries.
func (r *recovered) forget(id uint64) {
	j, ok := r.jobs[id]
	if !ok {
		return
	}
	for i := uint32(0); i < j.spec.n; i++ {
		delete(r.done, wire.TaskID{Job: id, Idx: i})
	}
	delete(r.jobs, id)
}

// snapshotResults is the log of a scheduler with no running jobs.
func snapshotResults(results map[uint64]finished) []wal.Record {
	recs := make([]wal.Record, 0, len(results))
	for id, f := range results {
		recs = append(recs, wal.JobDone{Job: id, Hash: f.result.Hash, Nonce: f.result.Nonce})
	}
	return recs
}
