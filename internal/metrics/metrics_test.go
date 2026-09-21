package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/neo1202/rudp-scheduler/rudp"
	"github.com/neo1202/rudp-scheduler/sched"
)

func render(s Snapshot) string {
	var b strings.Builder
	WritePrometheus(&b, s)
	return b.String()
}

func mustContain(t *testing.T, text string, lines ...string) {
	t.Helper()
	for _, l := range lines {
		if !strings.Contains(text, l+"\n") {
			t.Errorf("output is missing %q", l)
		}
	}
}

func TestWritePrometheus(t *testing.T) {
	var agg sched.AggStats
	agg.Merged, agg.Duplicates, agg.Orphans = 100, 7, 3
	agg.JobsStarted, agg.JobsCompleted, agg.JobsDropped, agg.ActiveJobs = 5, 3, 1, 1
	agg.JobLatency.Buckets[2] = 2 // <= 0.05
	agg.JobLatency.Buckets[5] = 1 // <= 0.5
	agg.JobLatency.Count, agg.JobLatency.Sum = 3, 0.42

	out := render(Snapshot{
		Conns: []rudp.ConnStats{
			{ConnID: 1, DataSent: 50, Retransmits: 4, SRTT: 250 * time.Microsecond, SendBuffer: 2},
			{ConnID: 2, DataSent: 10, Retransmits: 1, PendingWrites: 3},
		},
		ClosedConns: ConnTotals{Count: 2, DataSent: 40, Retransmits: 5},
		Workers: []sched.WorkerStats{
			{Worker: 1, Inflight: 5, Busy: 9 * time.Second, Alive: 10 * time.Second, SRTT: 20 * time.Millisecond, Sent: 60, Speculated: 2},
		},
		GoneWorkers: WorkerTotals{Count: 1, Sent: 30, Speculated: 1, Requeued: 5, DupResults: 4},
		Agg:         agg,
		NQ:          123, PQ: 4,
	})
	mustContain(t, out,
		`rudp_connections 2`,
		`rudp_connections_closed_total 2`,
		`rudp_data_sent_total 100`,
		`rudp_retransmits_total 10`,
		`rudp_conn_retransmits_total{conn="1",side="server"} 4`,
		`rudp_conn_srtt_seconds{conn="1",side="server"} 0.00025`,
		`rudp_conn_pending_writes{conn="2",side="server"} 3`,
		`sched_queue_depth{queue="nq"} 123`,
		`sched_queue_depth{queue="pq"} 4`,
		`sched_worker_inflight{worker="1"} 5`,
		`sched_worker_utilization{worker="1"} 0.9`,
		`sched_tasks_sent_total 90`,
		`sched_speculations_total 3`,
		`sched_tasks_requeued_total 5`,
		`sched_worker_duplicate_results_total 4`,
		`sched_results_merged_total 100`,
		`sched_results_duplicate_total 7`,
		`sched_results_orphan_total 3`,
		`sched_jobs_total{outcome="completed"} 3`,
		`sched_job_latency_seconds_bucket{le="0.025"} 0`,
		`sched_job_latency_seconds_bucket{le="0.05"} 2`,
		`sched_job_latency_seconds_bucket{le="0.25"} 2`,
		`sched_job_latency_seconds_bucket{le="0.5"} 3`,
		`sched_job_latency_seconds_bucket{le="+Inf"} 3`,
		`sched_job_latency_seconds_count 3`,
		`# TYPE sched_job_latency_seconds histogram`,
	)
}

func waitUntil(t *testing.T, c *Collector, cond func(Snapshot) bool) Snapshot {
	t.Helper()
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		if s := c.Snapshot(); cond(s) {
			return s
		}
	}
	t.Fatal("collector never reached the expected state")
	return Snapshot{}
}

// Counters must stay monotonic when the thing that owned them goes away.
func TestCollectorKeepsTotalsOfDepartedOwners(t *testing.T) {
	c := New()
	c.Start(func() (int, int) { return 7, 1 })

	c.ConnStats() <- rudp.ConnStats{ConnID: 1, DataSent: 10, Retransmits: 2}
	c.ConnStats() <- rudp.ConnStats{ConnID: 1, DataSent: 25, Retransmits: 3} // newer snapshot replaces
	c.ConnStats() <- rudp.ConnStats{ConnID: 2, DataSent: 5}
	c.WorkerStats() <- sched.WorkerStats{Worker: 1, Sent: 9, Speculated: 2}
	c.AggStats() <- sched.AggStats{Merged: 8}
	s := waitUntil(t, c, func(s Snapshot) bool { return len(s.Conns) == 2 && len(s.Workers) == 1 && s.Agg.Merged == 8 })
	if s.DataSent() != 30 || s.Retransmits() != 3 || s.NQ != 7 || s.PQ != 1 {
		t.Errorf("live totals: sent=%d retransmits=%d nq=%d pq=%d", s.DataSent(), s.Retransmits(), s.NQ, s.PQ)
	}

	c.ConnStats() <- rudp.ConnStats{ConnID: 1, DataSent: 26, Retransmits: 4, Closed: true}
	c.WorkerStats() <- sched.WorkerStats{Worker: 1, Sent: 12, Speculated: 3, Requeued: 5, Gone: true}
	s = waitUntil(t, c, func(s Snapshot) bool { return len(s.Conns) == 1 && len(s.Workers) == 0 })
	if s.DataSent() != 31 || s.Retransmits() != 4 || s.ClosedConns.Count != 1 {
		t.Errorf("after close: sent=%d retransmits=%d closed=%d", s.DataSent(), s.Retransmits(), s.ClosedConns.Count)
	}
	if wt := s.WorkerTotals(); wt.Sent != 12 || wt.Speculated != 3 || wt.Requeued != 5 || wt.Count != 1 {
		t.Errorf("after worker left: %+v", wt)
	}

	srv := httptest.NewServer(c.Handler())
	defer srv.Close()
	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	mustContain(t, string(body), `rudp_data_sent_total 31`, `sched_speculations_total 3`, `sched_results_merged_total 8`)

	c.Stop()
	c.Stop() // idempotent
	if got := c.Snapshot(); len(got.Conns) != 0 || got.Agg.Merged != 0 {
		t.Errorf("Snapshot after Stop = %+v, want zero", got)
	}
}
