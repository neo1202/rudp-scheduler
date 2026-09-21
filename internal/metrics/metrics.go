// Package metrics turns the snapshots published by the transport and the
// scheduler into Prometheus text, without breaking the repository's rule that
// every piece of mutable state has exactly one owner.
//
// Counters are never shared. Each connection's mainLoop, each workerLoop and
// the aggregator keep their own cumulative counters and publish copies over
// channels with non-blocking sends. One collector goroutine owns the latest
// copy of everything; HTTP handlers, which run on their own goroutines, ask
// it for a Snapshot over a channel. No atomics, no mutexes.
package metrics

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"

	"github.com/neo1202/rudp-scheduler/rudp"
	"github.com/neo1202/rudp-scheduler/sched"
)

// staleAfter is how long a series survives without a fresh snapshot. Live
// owners publish at least once a second; a final snapshot removes the series
// at once, this only covers the rare final snapshot lost to a full channel.
const staleAfter = 5 * time.Second

// ConnTotals accumulates the counters of connections that have ended.
type ConnTotals struct {
	Count        uint64
	DataSent     uint64
	Retransmits  uint64
	DataReceived uint64
	Heartbeats   uint64
}

// WorkerTotals accumulates the counters of workers that have left.
type WorkerTotals struct {
	Count      uint64
	Sent       uint64
	Results    uint64
	DupResults uint64
	Speculated uint64
	Requeued   uint64
}

// Snapshot is a consistent copy of everything the collector knows.
type Snapshot struct {
	Conns       []rudp.ConnStats // live connections
	ClosedConns ConnTotals
	Workers     []sched.WorkerStats // live workers
	GoneWorkers WorkerTotals
	Agg         sched.AggStats
	NQ, PQ      int
}

// DataSent returns first transmissions across live and ended connections.
func (s Snapshot) DataSent() uint64 {
	n := s.ClosedConns.DataSent
	for _, c := range s.Conns {
		n += c.DataSent
	}
	return n
}

// Retransmits returns retransmissions across live and ended connections.
func (s Snapshot) Retransmits() uint64 {
	n := s.ClosedConns.Retransmits
	for _, c := range s.Conns {
		n += c.Retransmits
	}
	return n
}

// WorkerTotals returns scheduler counters across live and departed workers.
func (s Snapshot) WorkerTotals() WorkerTotals {
	t := s.GoneWorkers
	for _, w := range s.Workers {
		t.Sent += w.Sent
		t.Results += w.Results
		t.DupResults += w.DupResults
		t.Speculated += w.Speculated
		t.Requeued += w.Requeued
	}
	return t
}

// Collector owns all metric state.
type Collector struct {
	connCh   chan rudp.ConnStats
	workerCh chan sched.WorkerStats
	aggCh    chan sched.AggStats
	scrapeCh chan chan Snapshot
	stopCh   chan struct{}
	done     chan struct{}
}

// New creates a collector. Hand its channels to the transport and scheduler
// parameters, then call Start.
func New() *Collector {
	return &Collector{
		connCh:   make(chan rudp.ConnStats, 4096),
		workerCh: make(chan sched.WorkerStats, 4096),
		aggCh:    make(chan sched.AggStats, 4096),
		scrapeCh: make(chan chan Snapshot),
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// ConnStats is where connections publish; use it as rudp.Params.Stats.
func (c *Collector) ConnStats() chan<- rudp.ConnStats { return c.connCh }

// WorkerStats is where workerLoops publish; use it as sched.Params.WorkerStats.
func (c *Collector) WorkerStats() chan<- sched.WorkerStats { return c.workerCh }

// AggStats is where the aggregator publishes; use it as sched.Params.AggStats.
func (c *Collector) AggStats() chan<- sched.AggStats { return c.aggCh }

// Start launches the collector goroutine. depths, if non-nil, reports the
// current queue depths and is called at scrape time.
func (c *Collector) Start(depths func() (nq, pq int)) { go c.run(depths) }

// Stop ends the collector goroutine. It may be called more than once.
func (c *Collector) Stop() {
	select {
	case c.stopCh <- struct{}{}:
	case <-c.done:
	}
	<-c.done
}

// Snapshot returns the collector's current view, or a zero Snapshot after Stop.
func (c *Collector) Snapshot() Snapshot {
	reply := make(chan Snapshot, 1)
	select {
	case c.scrapeCh <- reply:
		return <-reply
	case <-c.done:
		return Snapshot{}
	}
}

// Handler serves the Prometheus text exposition format.
func (c *Collector) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		WritePrometheus(w, c.Snapshot())
	})
}

type connKey struct {
	client bool
	id     int
}

type liveConn struct {
	stats rudp.ConnStats
	seen  time.Time
}

type liveWorker struct {
	stats sched.WorkerStats
	seen  time.Time
}

func (c *Collector) run(depths func() (int, int)) {
	defer close(c.done)
	var (
		conns       = map[connKey]liveConn{} // only this goroutine touches these
		workers     = map[int]liveWorker{}
		closedConns ConnTotals
		goneWorkers WorkerTotals
		agg         sched.AggStats
	)
	retireConn := func(k connKey, s rudp.ConnStats) {
		delete(conns, k)
		closedConns.Count++
		closedConns.DataSent += s.DataSent
		closedConns.Retransmits += s.Retransmits
		closedConns.DataReceived += s.DataReceived
		closedConns.Heartbeats += s.Heartbeats
	}
	retireWorker := func(id int, s sched.WorkerStats) {
		delete(workers, id)
		goneWorkers.Count++
		goneWorkers.Sent += s.Sent
		goneWorkers.Results += s.Results
		goneWorkers.DupResults += s.DupResults
		goneWorkers.Speculated += s.Speculated
		goneWorkers.Requeued += s.Requeued
	}

	for {
		select {
		case s := <-c.connCh:
			k := connKey{s.Client, s.ConnID}
			if s.Closed {
				retireConn(k, s)
			} else {
				conns[k] = liveConn{s, time.Now()}
			}
		case s := <-c.workerCh:
			if s.Gone {
				retireWorker(s.Worker, s)
			} else {
				workers[s.Worker] = liveWorker{s, time.Now()}
			}
		case agg = <-c.aggCh:
		case reply := <-c.scrapeCh:
			now := time.Now()
			for k, lc := range conns {
				if now.Sub(lc.seen) > staleAfter {
					retireConn(k, lc.stats)
				}
			}
			for id, lw := range workers {
				if now.Sub(lw.seen) > staleAfter {
					retireWorker(id, lw.stats)
				}
			}
			snap := Snapshot{ClosedConns: closedConns, GoneWorkers: goneWorkers, Agg: agg}
			for _, lc := range conns {
				snap.Conns = append(snap.Conns, lc.stats)
			}
			for _, lw := range workers {
				snap.Workers = append(snap.Workers, lw.stats)
			}
			sort.Slice(snap.Conns, func(i, j int) bool {
				a, b := snap.Conns[i], snap.Conns[j]
				if a.Client != b.Client {
					return !a.Client
				}
				return a.ConnID < b.ConnID
			})
			sort.Slice(snap.Workers, func(i, j int) bool { return snap.Workers[i].Worker < snap.Workers[j].Worker })
			if depths != nil {
				snap.NQ, snap.PQ = depths()
			}
			reply <- snap
		case <-c.stopCh:
			return
		}
	}
}

// WritePrometheus renders s in the Prometheus text exposition format.
func WritePrometheus(w io.Writer, s Snapshot) {
	metric := func(name, typ, help string) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
	}
	side := func(c rudp.ConnStats) string {
		if c.Client {
			return "client"
		}
		return "server"
	}
	perConn := func(name, typ, help string, value func(rudp.ConnStats) string) {
		metric(name, typ, help)
		for _, c := range s.Conns {
			fmt.Fprintf(w, "%s{conn=\"%d\",side=\"%s\"} %s\n", name, c.ConnID, side(c), value(c))
		}
	}
	perWorker := func(name, typ, help string, value func(sched.WorkerStats) string) {
		metric(name, typ, help)
		for _, wk := range s.Workers {
			fmt.Fprintf(w, "%s{worker=\"%d\"} %s\n", name, wk.Worker, value(wk))
		}
	}
	u := func(v uint64) string { return fmt.Sprintf("%d", v) }
	f := func(v float64) string { return fmt.Sprintf("%g", v) }

	// transport
	metric("rudp_connections", "gauge", "Open connections.")
	fmt.Fprintf(w, "rudp_connections %d\n", len(s.Conns))
	metric("rudp_connections_closed_total", "counter", "Connections that have ended.")
	fmt.Fprintf(w, "rudp_connections_closed_total %d\n", s.ClosedConns.Count)
	metric("rudp_data_sent_total", "counter", "Messages sent for the first time, all connections.")
	fmt.Fprintf(w, "rudp_data_sent_total %d\n", s.DataSent())
	metric("rudp_retransmits_total", "counter", "Message retransmissions, all connections.")
	fmt.Fprintf(w, "rudp_retransmits_total %d\n", s.Retransmits())
	perConn("rudp_conn_data_sent_total", "counter", "Messages sent for the first time on this connection.",
		func(c rudp.ConnStats) string { return u(c.DataSent) })
	perConn("rudp_conn_retransmits_total", "counter", "Message retransmissions on this connection.",
		func(c rudp.ConnStats) string { return u(c.Retransmits) })
	perConn("rudp_conn_data_received_total", "counter", "Data packets received on this connection, duplicates included.",
		func(c rudp.ConnStats) string { return u(c.DataReceived) })
	perConn("rudp_conn_srtt_seconds", "gauge", "Smoothed round-trip time of this connection.",
		func(c rudp.ConnStats) string { return f(c.SRTT.Seconds()) })
	perConn("rudp_conn_send_buffer", "gauge", "Unacknowledged messages in flight (at most the window).",
		func(c rudp.ConnStats) string { return fmt.Sprint(c.SendBuffer) })
	perConn("rudp_conn_pending_writes", "gauge", "Writes waiting for a window slot.",
		func(c rudp.ConnStats) string { return fmt.Sprint(c.PendingWrites) })

	// scheduler
	metric("sched_queue_depth", "gauge", "Tasks waiting in the normal (nq) and priority (pq) queues.")
	fmt.Fprintf(w, "sched_queue_depth{queue=\"nq\"} %d\nsched_queue_depth{queue=\"pq\"} %d\n", s.NQ, s.PQ)
	metric("sched_workers", "gauge", "Connected workers.")
	fmt.Fprintf(w, "sched_workers %d\n", len(s.Workers))
	perWorker("sched_worker_inflight", "gauge", "Tasks in flight at this worker (at most the window).",
		func(wk sched.WorkerStats) string { return fmt.Sprint(wk.Inflight) })
	perWorker("sched_worker_busy_seconds_total", "counter", "Time this worker has had at least one task in flight.",
		func(wk sched.WorkerStats) string { return f(wk.Busy.Seconds()) })
	perWorker("sched_worker_utilization", "gauge", "Fraction of its lifetime this worker has had at least one task in flight.",
		func(wk sched.WorkerStats) string {
			if wk.Alive <= 0 {
				return "0"
			}
			return f(wk.Busy.Seconds() / wk.Alive.Seconds())
		})
	perWorker("sched_worker_task_srtt_seconds", "gauge", "Smoothed task round-trip time of this worker.",
		func(wk sched.WorkerStats) string { return f(wk.SRTT.Seconds()) })

	wt := s.WorkerTotals()
	metric("sched_tasks_sent_total", "counter", "Chunks handed to workers, re-issues included.")
	fmt.Fprintf(w, "sched_tasks_sent_total %d\n", wt.Sent)
	metric("sched_speculations_total", "counter", "Straggling tasks cloned into the priority queue.")
	fmt.Fprintf(w, "sched_speculations_total %d\n", wt.Speculated)
	metric("sched_tasks_requeued_total", "counter", "Tasks handed back because their worker was lost.")
	fmt.Fprintf(w, "sched_tasks_requeued_total %d\n", wt.Requeued)
	metric("sched_worker_duplicate_results_total", "counter", "Results absorbed by a workerLoop because the task was no longer in flight (transport duplicates).")
	fmt.Fprintf(w, "sched_worker_duplicate_results_total %d\n", wt.DupResults)

	metric("sched_results_merged_total", "counter", "Results counted by the aggregator, exactly one per task.")
	fmt.Fprintf(w, "sched_results_merged_total %d\n", s.Agg.Merged)
	metric("sched_results_duplicate_total", "counter", "Results the aggregator dropped because the task was already counted.")
	fmt.Fprintf(w, "sched_results_duplicate_total %d\n", s.Agg.Duplicates)
	metric("sched_results_orphan_total", "counter", "Results the aggregator dropped because their job no longer exists.")
	fmt.Fprintf(w, "sched_results_orphan_total %d\n", s.Agg.Orphans)
	metric("sched_jobs_active", "gauge", "Jobs waiting for results.")
	fmt.Fprintf(w, "sched_jobs_active %d\n", s.Agg.ActiveJobs)
	metric("sched_jobs_total", "counter", "Jobs by outcome.")
	fmt.Fprintf(w, "sched_jobs_total{outcome=\"started\"} %d\n", s.Agg.JobsStarted)
	fmt.Fprintf(w, "sched_jobs_total{outcome=\"completed\"} %d\n", s.Agg.JobsCompleted)
	fmt.Fprintf(w, "sched_jobs_total{outcome=\"dropped\"} %d\n", s.Agg.JobsDropped)

	metric("sched_job_latency_seconds", "histogram", "Time from a job's registration to its Result.")
	cumulative := uint64(0)
	for i, bound := range sched.LatencyBounds {
		cumulative += s.Agg.JobLatency.Buckets[i]
		fmt.Fprintf(w, "sched_job_latency_seconds_bucket{le=\"%g\"} %d\n", bound, cumulative)
	}
	fmt.Fprintf(w, "sched_job_latency_seconds_bucket{le=\"+Inf\"} %d\n", s.Agg.JobLatency.Count)
	fmt.Fprintf(w, "sched_job_latency_seconds_sum %g\n", s.Agg.JobLatency.Sum)
	fmt.Fprintf(w, "sched_job_latency_seconds_count %d\n", s.Agg.JobLatency.Count)
}
