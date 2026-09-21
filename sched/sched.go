// Package sched is the scheduling layer of the server. It splits a client's
// request into independent tasks, lets workers pull them at their own pace,
// re-issues tasks that fall behind or whose worker died, and folds the results
// into a single answer.
//
// It never sees a sequence number, an acknowledgement or a retransmission:
// packet loss is entirely the transport's problem. In return, the transport
// only promises at-least-once, unordered delivery, so everything here is
// idempotent, keyed by TaskID.
//
// Four kinds of goroutine, no shared mutable state, no locks:
//
//	dispatcher     x1          owns the worker and client tables
//	clientHandler  x1/client   registers the job, splits it, exits
//	workerLoop     x1/worker   owns that worker's in-flight map and task SRTT
//	aggregator     x1          owns done and jobs
package sched

import (
	"errors"
	"math"
	"time"

	"github.com/neo1202/rudp-scheduler/transport"
	"github.com/neo1202/rudp-scheduler/wire"
	"github.com/neo1202/rudp-scheduler/workload"
)

// Task is one independent slice of a job. It is a value, and Msg is an
// immutable string, so handing a Task to another goroutine, or cloning it for
// speculation, is plain assignment.
type Task struct {
	ID     wire.TaskID
	Msg    string
	Lo, Hi uint64
}

// jobSpec is what is needed to split a job into tasks, now or after a restart.
type jobSpec struct {
	id     uint64
	lo, hi uint64
	size   uint64 // chunk width
	n      uint32 // number of tasks
	msg    string
}

// job is the aggregation state of one request. Only the aggregator touches it.
type job struct {
	spec       jobSpec
	client     transport.Conn // nil while no client is attached
	remaining  uint32
	acc        workload.Partial
	started    time.Time
	orphanedAt time.Time     // when the client was lost; zero while attached
	cancel     chan struct{} // closed when the job is dropped, stops its feeder
}

// submitReq is a clientHandler asking the aggregator what to do with a Request.
type submitReq struct {
	h     transport.Conn
	spec  jobSpec
	reply chan<- submitReply
}

// submitReply tells the clientHandler whether the job is new, in which case it
// must feed the tasks into nq until cancel is closed.
type submitReply struct {
	feed   bool
	cancel <-chan struct{}
}

// Scheduler wires the four kinds of goroutine together.
type Scheduler struct {
	srv    transport.Server
	wl     workload.Workload
	p      *Params
	window int

	nq chan Task // tasks not handed out yet; a full nq blocks the splitter (backpressure)
	pq chan Task // clones of stragglers and tasks handed back by lost workers; pulled first

	submitCh     chan submitReq        // clientHandler -> aggregator
	resultsCh    chan wire.ChunkResult // workerLoop -> aggregator
	clientGoneCh chan int              // dispatcher -> aggregator
	stop         chan struct{}         // closed by the dispatcher when the server is closed

	boot *recovered // state rebuilt from the log; handed to the aggregator at start
}

// New prepares a scheduler on top of srv. p may be nil. The scheduler only
// sees the transport.Server interface; for the reliable-UDP transport, srv
// should have been created from p.Transport so both layers agree on the window. With
// p.WALPath set, New replays the log; the jobs found there resume in Run.
func New(srv transport.Server, wl workload.Workload, p *Params) (*Scheduler, error) {
	p = p.withDefaults()
	s := &Scheduler{
		srv:          srv,
		wl:           wl,
		p:            p,
		window:       p.Transport.Window,
		nq:           make(chan Task, p.NQCap),
		pq:           make(chan Task, p.MaxWorkers*p.Transport.Window),
		submitCh:     make(chan submitReq),
		resultsCh:    make(chan wire.ChunkResult),
		clientGoneCh: make(chan int),
		stop:         make(chan struct{}),
	}
	boot, err := s.recover()
	if err != nil {
		return nil, err
	}
	s.boot = boot
	return s, nil
}

// QueueDepths reports how many tasks are waiting in the normal and the
// priority queue. It is safe to call from any goroutine.
func (s *Scheduler) QueueDepths() (nq, pq int) { return len(s.nq), len(s.pq) }

// Run starts the aggregator and then serves as the dispatcher. It returns
// after the server has been closed, having told every other scheduler
// goroutine to stop.
func (s *Scheduler) Run() {
	boot := s.boot
	s.boot = nil // from here on the state belongs to the aggregator
	for _, f := range boot.feeds {
		go s.feed(f.spec, f.skip, f.cancel) // resume what the log says was unfinished
	}
	go s.aggregator(boot)
	s.dispatcher()
}

// dispatcher is the only caller of srv.Read, which does not distinguish
// connections. It looks at who a message is from and passes it on. The first
// message of a connection decides what it is: Join spawns a workerLoop,
// Request spawns a clientHandler. Because the transport may deliver a message
// twice, every branch is idempotent. The dispatcher itself never does
// anything that could block for long; splitting a job can, so that is the
// clientHandler's task.
func (s *Scheduler) dispatcher() {
	defer close(s.stop)
	workers := map[int]*worker{} // only the dispatcher touches these
	clients := map[int]bool{}
	for {
		h, payload, err := s.srv.Read() // blocks until any connection has something
		if err != nil {
			if h == nil || errors.Is(err, transport.ErrClosed) {
				return
			}
			if w, ok := workers[h.ID()]; ok { // this connection is gone
				close(w.dead) // the workerLoop hands its in-flight tasks back itself
				delete(workers, h.ID())
			} else if clients[h.ID()] {
				delete(clients, h.ID())
				s.clientGoneCh <- h.ID() // the aggregator forgets the job
			}
			continue
		}
		m, err := wire.Decode(payload)
		if err != nil {
			continue
		}
		switch m := m.(type) {
		case wire.Join:
			if _, dup := workers[h.ID()]; dup || clients[h.ID()] {
				continue
			}
			w := newWorker(h, s.window)
			workers[h.ID()] = w
			go s.workerLoop(w)
		case wire.Request:
			if _, isWorker := workers[h.ID()]; isWorker || clients[h.ID()] {
				continue
			}
			clients[h.ID()] = true
			go s.clientHandler(h, m)
		case wire.ChunkResult:
			if w, ok := workers[h.ID()]; ok {
				w.resultCh <- m
			}
		}
	}
}

// clientHandler asks the aggregator about the request first, so that results
// can find the job, and then, if the job is new, pushes every task into nq and
// exits. Sending the Result is the aggregator's job. If the job is already
// known (the client reconnected, or the server restarted and recovered it)
// there is nothing to split and the handler exits at once.
func (s *Scheduler) clientHandler(h transport.Conn, req wire.Request) {
	n, size := split(req.Lo, req.Hi, s.p.ChunkSize)
	spec := jobSpec{id: req.Job, lo: req.Lo, hi: req.Hi, size: size, n: n, msg: req.Msg}
	reply := make(chan submitReply, 1)
	select {
	case s.submitCh <- submitReq{h: h, spec: spec, reply: reply}:
	case <-s.stop:
		return
	}
	select {
	case r := <-reply:
		if r.feed {
			s.feed(spec, nil, r.cancel)
		}
	case <-s.stop:
	}
}

// feed pushes a job's tasks into nq, skipping the ones in skip (already
// counted before a restart). A full nq blocks here on purpose: that is the
// backpressure that keeps a huge request from unfolding into memory.
func (s *Scheduler) feed(spec jobSpec, skip map[uint32]bool, cancel <-chan struct{}) {
	for i := uint32(0); i < spec.n; i++ {
		if skip[i] {
			continue
		}
		lo := spec.lo + uint64(i)*spec.size
		hi := spec.hi
		if hi-lo > spec.size {
			hi = lo + spec.size
		}
		t := Task{ID: wire.TaskID{Job: spec.id, Idx: i}, Msg: spec.msg, Lo: lo, Hi: hi}
		select {
		case s.nq <- t:
		case <-cancel: // job dropped: no point queueing the rest
			return
		case <-s.stop:
			return
		}
	}
}

// split decides how many tasks cover [lo, hi) and how wide each one is. The
// width is chunkSize unless that would exceed maxChunksPerJob tasks.
func split(lo, hi, chunkSize uint64) (n uint32, size uint64) {
	if hi <= lo {
		return 0, chunkSize
	}
	span := hi - lo
	size = chunkSize
	if span/size >= maxChunksPerJob {
		size = span/maxChunksPerJob + 1
	}
	count := span / size
	if span%size != 0 {
		count++
	}
	if count > math.MaxUint32 {
		count = math.MaxUint32 // unreachable given maxChunksPerJob; keeps the cast honest
	}
	return uint32(count), size
}
