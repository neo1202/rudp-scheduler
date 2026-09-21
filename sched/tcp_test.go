package sched_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/neo1202/rudp-scheduler/node"
	"github.com/neo1202/rudp-scheduler/sched"
	"github.com/neo1202/rudp-scheduler/transport"
	"github.com/neo1202/rudp-scheduler/workload"
)

// The scheduler only knows the transport interface, so the same guarantees
// must hold on TCP: a worker dies mid-job, another joins, the answer is right
// and every task is counted once.
func TestSchedulerOverTCP(t *testing.T) {
	leakCheck(t)
	const tasks = 300
	const job = 99
	srv, err := transport.ListenTCP(0)
	if err != nil {
		t.Fatal(err)
	}
	addr := fmt.Sprintf("127.0.0.1:%d", srv.Port())
	pr := newProbe()
	p := testParams()
	p.Hooks = pr.hooks()
	s, err := sched.New(srv, hs, p)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan struct{})
	go func() { s.Run(); close(runDone) }()

	var workers []*transport.TCPClient
	exited := make(chan error, 8)
	addWorker := func() *transport.TCPClient {
		w, err := transport.DialTCP(addr)
		if err != nil {
			t.Fatal(err)
		}
		workers = append(workers, w)
		go func() { exited <- node.RunWorker(w, &workload.Paced{Inner: hs, MinCost: 4 * time.Millisecond}) }()
		return w
	}
	for i := 0; i < 3; i++ {
		addWorker()
	}

	cl, err := transport.DialTCP(addr)
	if err != nil {
		t.Fatal(err)
	}
	hi := uint64(tasks) * p.ChunkSize
	result := make(chan workload.Partial, 1)
	go func() {
		res, err := node.Submit(cl, job, "over tcp", 0, hi)
		if err != nil {
			t.Error(err)
		}
		result <- res
	}()

	waitFor(t, "progress", 5*time.Second, func() bool { n, _ := pr.mergedFor(job); return n >= 30 })
	workers[0].Close() // a worker leaves with chunks in flight
	addWorker()

	if got, want := <-result, hs.Compute("over tcp", 0, hi); got != want {
		t.Errorf("result = %+v, want %+v", got, want)
	}
	pr.assertExactlyOnce(t, job, tasks)

	cl.Close()
	for _, w := range workers {
		w.Close()
	}
	srv.Close()
	<-runDone
	for range workers {
		<-exited
	}
}
