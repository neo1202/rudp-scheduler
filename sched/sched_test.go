package sched_test

import (
	"fmt"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/neo1202/rudp-scheduler/internal/lossy"
	"github.com/neo1202/rudp-scheduler/node"
	"github.com/neo1202/rudp-scheduler/rudp"
	"github.com/neo1202/rudp-scheduler/sched"
	"github.com/neo1202/rudp-scheduler/wire"
	"github.com/neo1202/rudp-scheduler/workload"
	"github.com/neo1202/rudp-scheduler/workload/hashsearch"
)

var hs hashsearch.Workload

func leakCheck(t *testing.T) {
	t.Helper()
	before := runtime.NumGoroutine()
	t.Cleanup(func() {
		deadline := time.Now().Add(5 * time.Second)
		for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if n := runtime.NumGoroutine(); n > before {
			buf := make([]byte, 1<<20)
			t.Errorf("goroutine leak: %d before, %d after\n%s", before, n, buf[:runtime.Stack(buf, true)])
		}
	})
}

// probe collects what the scheduler's hooks report. The hooks run on the
// scheduler's goroutines, so the test side needs a lock; the scheduler doesn't.
type probe struct {
	mu          sync.Mutex
	merges      map[wire.TaskID]int
	speculated  map[string]int // "worker/client/idx" -> times cloned
	duplicates  int
	orphans     int
	maxInflight int
	jobEnds     map[int]bool // client -> completed
	backlogged  map[int]int  // transport conn -> worst pendingWrites depth
}

func newProbe() *probe {
	return &probe{
		merges:     map[wire.TaskID]int{},
		speculated: map[string]int{},
		jobEnds:    map[int]bool{},
		backlogged: map[int]int{},
	}
}

func (pr *probe) locked(f func()) { pr.mu.Lock(); defer pr.mu.Unlock(); f() }

func (pr *probe) hooks() *sched.Hooks {
	return &sched.Hooks{
		OnSend: func(_ int, _ wire.TaskID, inflight int) {
			pr.locked(func() {
				if inflight > pr.maxInflight {
					pr.maxInflight = inflight
				}
			})
		},
		OnSpeculate: func(w int, id wire.TaskID) {
			pr.locked(func() { pr.speculated[fmt.Sprintf("%d/%d/%d", w, id.Client, id.Idx)]++ })
		},
		OnMerge: func(id wire.TaskID) { pr.locked(func() { pr.merges[id]++ }) },
		OnDiscard: func(_ wire.TaskID, dup bool) {
			pr.locked(func() {
				if dup {
					pr.duplicates++
				} else {
					pr.orphans++
				}
			})
		},
		OnJobEnd: func(c int, completed bool) { pr.locked(func() { pr.jobEnds[c] = completed }) },
	}
}

// assertExactlyOnce checks that every task of the client's job was merged
// exactly once.
func (pr *probe) assertExactlyOnce(t *testing.T, client, tasks int) {
	t.Helper()
	pr.locked(func() {
		seen := 0
		for id, n := range pr.merges {
			if int(id.Client) != client {
				continue
			}
			seen++
			if n != 1 {
				t.Errorf("task %+v was merged %d times, want exactly 1", id, n)
			}
		}
		if seen != tasks {
			t.Errorf("client %d: %d distinct tasks merged, want %d", client, seen, tasks)
		}
	})
}

type cluster struct {
	t       *testing.T
	p       *sched.Params
	srv     *rudp.Server
	sch     *sched.Scheduler
	addr    string
	probe   *probe
	net     lossy.Config
	seedMu  sync.Mutex
	seed    int64
	runDone chan struct{}
	workers []*testWorker
}

// testParams keeps connections alive through bad luck and slow CI machines
// while still detecting a killed peer within 300 ms.
func testParams() *sched.Params {
	return &sched.Params{
		Transport: rudp.Params{EpochMs: 10, EpochLimit: 30},
		ChunkSize: 500,
	}
}

func startCluster(t *testing.T, p *sched.Params, network lossy.Config) *cluster {
	t.Helper()
	c := &cluster{t: t, p: p, probe: newProbe(), net: network, seed: 100, runDone: make(chan struct{})}
	p.Hooks = c.probe.hooks()
	tp := p.Transport
	tp.WrapSocket, _ = c.wrap()
	tp.Hooks = &rudp.Hooks{OnBacklog: func(conn, depth int) {
		c.probe.locked(func() {
			if depth > c.probe.backlogged[conn] {
				c.probe.backlogged[conn] = depth
			}
		})
	}}
	srv, err := rudp.NewServer(0, &tp)
	if err != nil {
		t.Fatal(err)
	}
	c.srv, c.addr = srv, fmt.Sprintf("127.0.0.1:%d", srv.Port())
	c.sch = sched.New(srv, hs, p)
	go func() { c.sch.Run(); close(c.runDone) }()
	return c
}

// wrap returns a socket wrapper applying the cluster's network conditions with
// a fresh seed, plus a way to yank the real socket out from under it.
func (c *cluster) wrap() (func(net.PacketConn) net.PacketConn, func()) {
	c.seedMu.Lock()
	c.seed++
	cfg := c.net
	cfg.Seed = c.seed
	c.seedMu.Unlock()
	var raw net.PacketConn
	return func(pc net.PacketConn) net.PacketConn {
		raw = pc
		return lossy.Wrap(pc, cfg)
	}, func() { raw.Close() }
}

type testWorker struct {
	id   int
	c    *rudp.Client
	kill func() // pull the plug: no goodbye, the server has to notice the silence
	done chan error
}

func (c *cluster) addWorker(wl workload.Workload) *testWorker {
	c.t.Helper()
	tp := c.p.Transport
	var kill func()
	tp.WrapSocket, kill = c.wrap()
	cl, err := rudp.NewClient(c.addr, &tp)
	if err != nil {
		c.t.Fatal(err)
	}
	w := &testWorker{id: cl.ID(), c: cl, kill: kill, done: make(chan error, 1)}
	go func() { w.done <- node.RunWorker(cl, wl) }()
	c.workers = append(c.workers, w)
	return w
}

type outcome struct {
	client int
	result workload.Partial
	err    error
	took   time.Duration
}

// submit runs one job on a fresh connection. kill, if non-nil, receives a
// function that pulls the plug on this client once it is connected.
func (c *cluster) submit(msg string, lo, hi uint64, kill chan<- func()) outcome {
	c.t.Helper()
	tp := c.p.Transport
	var k func()
	tp.WrapSocket, k = c.wrap()
	cl, err := rudp.NewClient(c.addr, &tp)
	if err != nil {
		c.t.Fatal(err)
	}
	defer cl.Close()
	if kill != nil {
		kill <- k
	}
	start := time.Now()
	res, err := node.Submit(cl, msg, lo, hi)
	return outcome{client: cl.ID(), result: res, err: err, took: time.Since(start)}
}

func (c *cluster) close() {
	for _, w := range c.workers {
		w.c.Close()
	}
	c.srv.Close()
	<-c.runDone
	for _, w := range c.workers {
		<-w.done
	}
}

func (c *cluster) assertWindowRespected() {
	c.t.Helper()
	c.probe.locked(func() {
		if w := rudp.DefaultWindow; c.probe.maxInflight > w {
			c.t.Errorf("invariant 1 broken: a worker had %d tasks in flight, window is %d", c.probe.maxInflight, w)
		}
	})
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

// Acceptance: exactly-once. 10% drop + 10% dup on every socket, 5 workers.
// The answer must equal the sequential one and every TaskID must be merged
// exactly once, however many times the network and the workers repeated it.
func TestExactlyOnceUnderLossAndDuplication(t *testing.T) {
	leakCheck(t)
	const tasks = 200
	c := startCluster(t, testParams(), lossy.Config{Drop: 0.10, Dup: 0.10, Jitter: time.Millisecond})
	defer c.close()
	for i := 0; i < 5; i++ {
		c.addWorker(hs)
	}

	hi := uint64(tasks) * c.p.ChunkSize
	got := c.submit("exactly once", 0, hi, nil)
	if got.err != nil {
		t.Fatal(got.err)
	}
	if want := hs.Compute("exactly once", 0, hi); got.result != want {
		t.Errorf("result = %+v, want %+v (sequential)", got.result, want)
	}
	c.probe.assertExactlyOnce(t, got.client, tasks)
	c.assertWindowRespected()
	c.probe.locked(func() {
		t.Logf("job took %v; aggregator discarded %d duplicate results", got.took, c.probe.duplicates)
	})
}

// Acceptance: kill two workers mid-job, add a new one, still get the right
// answer. The killed workers' in-flight tasks must come back through pq.
func TestWorkersDieAndJoinMidJob(t *testing.T) {
	leakCheck(t)
	const tasks = 300
	p := testParams()
	stats := make(chan sched.WorkerStats, 1<<14)
	p.WorkerStats = stats
	c := startCluster(t, p, lossy.Config{Drop: 0.05})
	defer c.close()
	slowish := func() workload.Workload { return &workload.Paced{Inner: hs, MinCost: 5 * time.Millisecond} }
	var ws []*testWorker
	for i := 0; i < 4; i++ {
		ws = append(ws, c.addWorker(slowish()))
	}

	hi := uint64(tasks) * c.p.ChunkSize
	result := make(chan outcome, 1)
	go func() { result <- c.submit("churn", 0, hi, nil) }()

	time.Sleep(100 * time.Millisecond)
	ws[0].kill()
	ws[1].kill()
	time.Sleep(50 * time.Millisecond)
	c.addWorker(slowish())

	got := <-result
	if got.err != nil {
		t.Fatal(got.err)
	}
	if want := hs.Compute("churn", 0, hi); got.result != want {
		t.Errorf("result = %+v, want %+v", got.result, want)
	}
	c.probe.assertExactlyOnce(t, got.client, tasks)
	c.assertWindowRespected()

	requeued := map[int]uint64{}
	for len(stats) > 0 {
		if s := <-stats; s.Gone {
			requeued[s.Worker] = s.Requeued
		}
	}
	for _, w := range ws[:2] {
		if requeued[w.id] == 0 {
			t.Errorf("killed worker %d handed back no tasks (final stats: %v)", w.id, requeued)
		}
	}
	t.Logf("job took %v; tasks handed back by dead workers: %v", got.took, requeued)
}

// Acceptance: a worker that turns 50x slower must not hold the job hostage.
// Speculation has to fire, and no in-flight entry may be cloned twice.
func TestStragglerDoesNotHoldTheJob(t *testing.T) {
	leakCheck(t)
	const tasks = 300
	run := func(noSpeculate bool) (time.Duration, *probe) {
		p := testParams()
		p.NoSpeculate = noSpeculate
		c := startCluster(t, p, lossy.Config{})
		defer c.close()
		for i := 0; i < 3; i++ {
			c.addWorker(&workload.Paced{Inner: hs, MinCost: 4 * time.Millisecond})
		}
		c.addWorker(&workload.Paced{Inner: hs, MinCost: 4 * time.Millisecond, Slowdown: 50, SlowAfter: 5})

		hi := uint64(tasks) * c.p.ChunkSize
		got := c.submit("straggler", 0, hi, nil)
		if got.err != nil {
			t.Fatal(got.err)
		}
		if want := hs.Compute("straggler", 0, hi); got.result != want {
			t.Errorf("noSpeculate=%v: result = %+v, want %+v", noSpeculate, got.result, want)
		}
		c.probe.assertExactlyOnce(t, got.client, tasks)
		c.assertWindowRespected()
		return got.took, c.probe
	}

	hedged, pr := run(false)
	unhedged, _ := run(true)
	t.Logf("job time with speculation %v, without %v", hedged, unhedged)

	if hedged > unhedged*7/10 {
		t.Errorf("speculation did not help: %v with, %v without", hedged, unhedged)
	}
	pr.locked(func() {
		if len(pr.speculated) == 0 {
			t.Error("speculation never fired")
		}
		for key, n := range pr.speculated {
			if n > 1 {
				t.Errorf("invariant 2 broken: in-flight entry %s was cloned %d times", key, n)
			}
		}
		t.Logf("clones issued: %d, duplicate results discarded: %d", len(pr.speculated), pr.duplicates)
	})
}

// Acceptance: a client that disappears mid-job. Its job is dropped, results
// that come back later are discarded, the server keeps serving, and nothing
// leaks once everything is shut down.
func TestClientVanishesMidJob(t *testing.T) {
	leakCheck(t)
	c := startCluster(t, testParams(), lossy.Config{})
	for i := 0; i < 2; i++ {
		c.addWorker(&workload.Paced{Inner: hs, MinCost: 5 * time.Millisecond})
	}

	killCh := make(chan func(), 1)
	doomed := make(chan outcome, 1)
	go func() { doomed <- c.submit("doomed", 0, 400*c.p.ChunkSize, killCh) }()
	kill := <-killCh
	waitFor(t, "the doomed job to make progress", 5*time.Second, func() bool {
		n := 0
		c.probe.locked(func() { n = len(c.probe.merges) })
		return n >= 10
	})
	kill()

	got := <-doomed
	if got.err == nil {
		t.Fatalf("doomed client got a result: %+v", got.result)
	}
	waitFor(t, "the aggregator to drop the job", 5*time.Second, func() bool {
		dropped := false
		c.probe.locked(func() {
			completed, ended := c.probe.jobEnds[got.client]
			dropped = ended && !completed
		})
		return dropped
	})
	waitFor(t, "late results to be discarded", 5*time.Second, func() bool {
		n := 0
		c.probe.locked(func() { n = c.probe.orphans })
		return n > 0
	})

	// The server must be unharmed: a new client gets a correct answer.
	next := c.submit("survivor", 0, 20*c.p.ChunkSize, nil)
	if next.err != nil {
		t.Fatal(next.err)
	}
	if want := hs.Compute("survivor", 0, 20*c.p.ChunkSize); next.result != want {
		t.Errorf("result after a client vanished = %+v, want %+v", next.result, want)
	}
	c.probe.assertExactlyOnce(t, next.client, 20)
	c.probe.locked(func() { t.Logf("orphan results discarded: %d", c.probe.orphans) })

	c.close() // leakCheck now verifies that every goroutine is gone
}

// Acceptance: invariants 1 and 6, asserted through hooks on a loss-free
// network. Invariant 6 (a worker connection never parks anything in
// pendingWrites) follows from invariant 1 only while an Ack cannot be
// overtaken by the result it precedes, i.e. without loss or reordering.
func TestWindowInvariants(t *testing.T) {
	leakCheck(t)
	c := startCluster(t, testParams(), lossy.Config{})
	defer c.close()
	workerConns := map[int]bool{}
	for i := 0; i < 3; i++ {
		workerConns[c.addWorker(hs).id] = true
	}
	got := c.submit("invariants", 0, 500*c.p.ChunkSize, nil)
	if got.err != nil {
		t.Fatal(got.err)
	}
	c.assertWindowRespected()
	c.probe.locked(func() {
		if c.probe.maxInflight != rudp.DefaultWindow {
			t.Errorf("workers peaked at %d in flight, expected them to fill the window of %d", c.probe.maxInflight, rudp.DefaultWindow)
		}
		for conn, depth := range c.probe.backlogged {
			if workerConns[conn] {
				t.Errorf("invariant 6 broken: worker connection %d had %d pendingWrites", conn, depth)
			}
		}
	})
}

func TestConcurrentClientsAndOddRanges(t *testing.T) {
	leakCheck(t)
	c := startCluster(t, testParams(), lossy.Config{Drop: 0.05, Dup: 0.05})
	defer c.close()
	for i := 0; i < 4; i++ {
		c.addWorker(hs)
	}

	cases := []struct {
		msg    string
		lo, hi uint64
		tasks  int
	}{
		{"alpha", 0, 50 * 500, 50},
		{"beta", 12345, 12345 + 33*500 + 7, 34}, // last chunk is a stub
		{"gamma", 999, 1000, 1},                 // smaller than one chunk
		{"empty", 42, 42, 0},                    // nothing to do
		{"inverted", 10, 5, 0},                  // hi < lo is an empty range too
	}
	var wg sync.WaitGroup
	for _, tc := range cases {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got := c.submit(tc.msg, tc.lo, tc.hi, nil)
			if got.err != nil {
				t.Errorf("%s: %v", tc.msg, got.err)
				return
			}
			if want := hs.Compute(tc.msg, tc.lo, tc.hi); got.result != want {
				t.Errorf("%s: result = %+v, want %+v", tc.msg, got.result, want)
			}
			c.probe.assertExactlyOnce(t, got.client, tc.tasks)
		}()
	}
	wg.Wait()
	c.assertWindowRespected()
}

// A Join or Request delivered twice must not create a second worker or job.
func TestDuplicateJoinAndRequestAreIdempotent(t *testing.T) {
	leakCheck(t)
	c := startCluster(t, testParams(), lossy.Config{})
	defer c.close()

	tp := c.p.Transport
	wc, err := rudp.NewClient(c.addr, &tp)
	if err != nil {
		t.Fatal(err)
	}
	wc.Write(wire.Encode(wire.Join{})) // RunWorker sends Join again
	w := &testWorker{id: wc.ID(), c: wc, done: make(chan error, 1)}
	go func() { w.done <- node.RunWorker(wc, hs) }()
	c.workers = append(c.workers, w)

	cl, err := rudp.NewClient(c.addr, &tp)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	const tasks = 10
	cl.Write(wire.Encode(wire.Request{Lo: 0, Hi: tasks * 500, Msg: "twice"})) // Submit sends it again
	got, err := node.Submit(cl, "twice", 0, tasks*500)
	if err != nil {
		t.Fatal(err)
	}
	if want := hs.Compute("twice", 0, tasks*500); got != want {
		t.Errorf("result = %+v, want %+v", got, want)
	}
	c.probe.assertExactlyOnce(t, cl.ID(), tasks)
	c.probe.locked(func() {
		if n := len(c.probe.jobEnds); n != 1 {
			t.Errorf("%d jobs ended, want 1", n)
		}
	})
}
