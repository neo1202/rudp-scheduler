package sched_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/neo1202/rudp-scheduler/internal/lossy"
	"github.com/neo1202/rudp-scheduler/sched"
	"github.com/neo1202/rudp-scheduler/workload"
)

func paced() workload.Workload {
	return &workload.Paced{Inner: hs, MinCost: 4 * time.Millisecond}
}

func (pr *probe) mergedFor(job uint64) (distinct, repeated int) {
	pr.locked(func() {
		for id, n := range pr.merges {
			if id.Job == job {
				distinct++
				if n > 1 {
					repeated++
				}
			}
		}
	})
	return
}

func lastAggStats(ch chan sched.AggStats) (s sched.AggStats) {
	for {
		select {
		case s = <-ch:
		default:
			return s
		}
	}
}

// The server dies mid-job. What a crash leaves on disk is whatever prefix of
// the log had reached the file, possibly ending in half a record. A new
// server started on that file must finish the job without redoing the chunks
// the log remembers, and the client, asking again with the same job ID, must
// get the same answer a single uninterrupted run would give.
func TestServerRestartResumesJob(t *testing.T) {
	leakCheck(t)
	const tasks = 300
	const job = 0xC0FFEE
	dir := t.TempDir()
	live, crashed := filepath.Join(dir, "live.wal"), filepath.Join(dir, "crashed.wal")

	p1 := testParams()
	p1.WALPath = live
	c1 := startCluster(t, p1, lossy.Config{Drop: 0.05})
	for i := 0; i < 3; i++ {
		c1.addWorker(paced())
	}
	hi := uint64(tasks) * p1.ChunkSize
	first := make(chan outcome, 1)
	go func() { first <- c1.submitAs(job, "restart", 0, hi, nil) }()

	waitFor(t, "the first server to get about a third through", 10*time.Second, func() bool {
		n, _ := c1.probe.mergedFor(job)
		return n >= tasks/3
	})
	data, err := os.ReadFile(live) // the moment of the crash
	if err != nil {
		t.Fatal(err)
	}
	if len(data) < 100 {
		t.Fatalf("log holds only %d bytes a third of the way through the job", len(data))
	}
	if err := os.WriteFile(crashed, data[:len(data)-5], 0o644); err != nil { // and a torn final record
		t.Fatal(err)
	}
	<-first // let the first cluster finish; its fate no longer matters
	c1.close()

	p2 := testParams()
	p2.WALPath = crashed
	p2.JobGraceMs = 5000
	agg := make(chan sched.AggStats, 1<<14)
	p2.AggStats = agg
	c2 := startCluster(t, p2, lossy.Config{Drop: 0.05})
	defer c2.close()
	for i := 0; i < 3; i++ {
		c2.addWorker(paced())
	}

	got := c2.submitAs(job, "restart", 0, hi, nil)
	if got.err != nil {
		t.Fatal(got.err)
	}
	if want := hs.Compute("restart", 0, hi); got.result != want {
		t.Errorf("result after restart = %+v, want %+v", got.result, want)
	}

	st := lastAggStats(agg)
	redone, repeated := c2.probe.mergedFor(job)
	if st.JobsRecovered != 1 || st.ChunksRecovered == 0 {
		t.Fatalf("recovered %d jobs and %d chunks from the log, want 1 job and some chunks", st.JobsRecovered, st.ChunksRecovered)
	}
	if repeated != 0 {
		t.Errorf("%d tasks were merged more than once after the restart", repeated)
	}
	if redone+int(st.ChunksRecovered) != tasks {
		t.Errorf("recovered %d + recomputed %d = %d, want exactly %d: a chunk was lost or counted twice",
			st.ChunksRecovered, redone, redone+int(st.ChunksRecovered), tasks)
	}
	if st.JobsStarted != 0 || st.Reattached != 1 {
		t.Errorf("JobsStarted=%d Reattached=%d, want the request to join the recovered job (0, 1)", st.JobsStarted, st.Reattached)
	}
	t.Logf("log remembered %d of %d chunks; %d recomputed", st.ChunksRecovered, tasks, redone)
}

// A client that loses its connection mid-job comes back with the same job ID
// and gets the answer, and no chunk is counted twice along the way.
func TestClientReattachesToRunningJob(t *testing.T) {
	leakCheck(t)
	const tasks = 300
	const job = 77
	p := testParams()
	p.JobGraceMs = 5000
	c := startCluster(t, p, lossy.Config{})
	defer c.close()
	for i := 0; i < 3; i++ {
		c.addWorker(paced())
	}
	hi := uint64(tasks) * p.ChunkSize

	killCh := make(chan func(), 1)
	doomed := make(chan outcome, 1)
	go func() { doomed <- c.submitAs(job, "reattach", 0, hi, killCh) }()
	kill := <-killCh
	waitFor(t, "the job to make progress", 5*time.Second, func() bool {
		n, _ := c.probe.mergedFor(job)
		return n >= 20
	})
	kill()
	if got := <-doomed; got.err == nil {
		t.Fatal("the killed client got a result")
	}

	got := c.submitAs(job, "reattach", 0, hi, nil)
	if got.err != nil {
		t.Fatal(got.err)
	}
	if want := hs.Compute("reattach", 0, hi); got.result != want {
		t.Errorf("result = %+v, want %+v", got.result, want)
	}
	c.probe.assertExactlyOnce(t, job, tasks)
	c.probe.locked(func() {
		if completed, ended := c.probe.jobEnds[job]; !ended || !completed {
			t.Errorf("job ended=%v completed=%v, want it completed once", ended, completed)
		}
	})

	// Asking yet again, after the job is done, returns the stored answer
	// without any new work.
	before, _ := c.probe.mergedFor(job)
	again := c.submitAs(job, "reattach", 0, hi, nil)
	if again.err != nil || again.result != got.result {
		t.Errorf("asking again: %+v, %v; want %+v", again.result, again.err, got.result)
	}
	if after, _ := c.probe.mergedFor(job); after != before {
		t.Errorf("asking again caused %d more merges", after-before)
	}
	if again.took > 500*time.Millisecond {
		t.Errorf("stored answer took %v", again.took)
	}
}

// A recovered job whose client never comes back must not live forever.
func TestRecoveredJobIsDroppedAfterGrace(t *testing.T) {
	leakCheck(t)
	const job = 5
	path := filepath.Join(t.TempDir(), "wal")

	p1 := testParams()
	p1.WALPath = path
	c1 := startCluster(t, p1, lossy.Config{}) // no workers: the job can only be logged
	killCh := make(chan func(), 1)
	go c1.submitAs(job, "abandoned", 0, 50*p1.ChunkSize, killCh)
	kill := <-killCh
	var data []byte
	waitFor(t, "JobStart to reach the log", 5*time.Second, func() bool {
		data, _ = os.ReadFile(path)
		return len(data) > 0
	})
	kill() // the client is gone for good; the server "crashes" with the job on disk
	snapshot := path + ".crash"
	os.WriteFile(snapshot, data, 0o644)
	c1.close()

	p2 := testParams()
	p2.WALPath = snapshot
	c2 := startCluster(t, p2, lossy.Config{})
	defer c2.close()
	waitFor(t, "the recovered job to be dropped", 5*time.Second, func() bool {
		dropped := false
		c2.probe.locked(func() {
			completed, ended := c2.probe.jobEnds[job]
			dropped = ended && !completed
		})
		return dropped
	})
}
