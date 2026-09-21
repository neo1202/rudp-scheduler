package main

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/neo1202/rudp-scheduler/internal/lossy"
	"github.com/neo1202/rudp-scheduler/internal/metrics"
	"github.com/neo1202/rudp-scheduler/node"
	"github.com/neo1202/rudp-scheduler/rudp"
	"github.com/neo1202/rudp-scheduler/sched"
	"github.com/neo1202/rudp-scheduler/workload"
	"github.com/neo1202/rudp-scheduler/workload/hashsearch"
)

// benchEpochLimit replaces the default of 5. A client waiting for its Result
// is kept alive by heartbeats alone; at 20% loss, five in a row vanish with
// probability 0.2^5 per epoch, which would end about one job in fifty for
// reasons that have nothing to do with what is being measured.
const benchEpochLimit = 10

// config describes one cluster.
type config struct {
	workers     int
	drop        float64
	chunkSize   uint64
	ordered     bool // transport delivers in sequence order (head-of-line blocking)
	noSpeculate bool
	straggler   bool // worker 0 turns 50x slower after its first stragglerAfter chunks
	seed        int64
}

const (
	stragglerFactor = 50
	stragglerAfter  = 10
)

// metered reports how long each Compute call took, so the harness can work
// out how busy the workers really were. It sends on a channel; nothing is
// shared.
type metered struct {
	workload.Workload
	busy chan<- time.Duration
}

func (m metered) Compute(msg string, lo, hi uint64) workload.Partial {
	start := time.Now()
	p := m.Workload.Compute(msg, lo, hi)
	m.busy <- time.Since(start)
	return p
}

type cluster struct {
	cfg     config
	tp      rudp.Params
	srv     *rudp.Server
	col     *metrics.Collector
	addr    string
	seed    int64
	busy    chan time.Duration
	workers []*rudp.Client
	exited  chan struct{}
	runDone chan struct{}
}

func startCluster(cfg config) *cluster {
	c := &cluster{
		cfg:     cfg,
		col:     metrics.New(),
		seed:    cfg.seed,
		busy:    make(chan time.Duration, 1<<18),
		exited:  make(chan struct{}, cfg.workers),
		runDone: make(chan struct{}),
	}
	p := &sched.Params{
		Transport: rudp.Params{
			EpochLimit:      benchEpochLimit,
			OrderedDelivery: cfg.ordered,
			Stats:           c.col.ConnStats(),
		},
		ChunkSize:   cfg.chunkSize,
		NoSpeculate: cfg.noSpeculate,
		WorkerStats: c.col.WorkerStats(),
		AggStats:    c.col.AggStats(),
	}
	c.tp = p.Transport

	st := c.tp
	st.WrapSocket = c.impair()
	srv, err := rudp.NewServer(0, &st)
	if err != nil {
		log.Fatal(err)
	}
	c.srv, c.addr = srv, fmt.Sprintf("127.0.0.1:%d", srv.Port())
	s, err := sched.New(srv, hashsearch.Workload{}, p)
	if err != nil {
		log.Fatal(err)
	}
	c.col.Start(s.QueueDepths)
	go func() { s.Run(); close(c.runDone) }()

	for i := 0; i < cfg.workers; i++ {
		var wl workload.Workload = hashsearch.Workload{}
		if cfg.straggler && i == 0 {
			wl = &workload.Paced{Inner: wl, Slowdown: stragglerFactor, SlowAfter: stragglerAfter}
		}
		wt := c.tp
		wt.WrapSocket = c.impair()
		wc, err := rudp.NewClient(c.addr, &wt)
		if err != nil {
			log.Fatal(err)
		}
		c.workers = append(c.workers, wc)
		go func() {
			node.RunWorker(wc, metered{wl, c.busy})
			c.exited <- struct{}{}
		}()
	}
	time.Sleep(5 * c.tp.Epoch()) // let every Join land before the first job
	return c
}

// impair returns a socket wrapper with the cluster's loss rate and the next
// seed in its sequence. With no loss configured the socket is left alone.
func (c *cluster) impair() func(net.PacketConn) net.PacketConn {
	c.seed++
	if c.cfg.drop <= 0 {
		return nil
	}
	cfg := lossy.Config{Drop: c.cfg.drop, Seed: c.seed}
	return func(pc net.PacketConn) net.PacketConn { return lossy.Wrap(pc, cfg) }
}

// job runs one request on a fresh connection and returns how long the client
// waited for its answer, connection setup excluded.
func (c *cluster) job(msg string, lo, hi uint64) (workload.Partial, time.Duration, error) {
	ct := c.tp
	ct.WrapSocket = c.impair()
	cl, err := rudp.NewClient(c.addr, &ct)
	if err != nil {
		return workload.Partial{}, 0, err
	}
	defer cl.Close()
	start := time.Now()
	res, err := node.Submit(cl, node.NewJobID(), msg, lo, hi)
	return res, time.Since(start), err
}

// drainBusy returns the compute time reported since the last call.
func (c *cluster) drainBusy() time.Duration {
	var total time.Duration
	for {
		select {
		case d := <-c.busy:
			total += d
		default:
			return total
		}
	}
}

// snapshot waits for the owners' next publication, then reads the counters.
func (c *cluster) snapshot() metrics.Snapshot {
	time.Sleep(3 * c.tp.Epoch())
	return c.col.Snapshot()
}

func (c *cluster) close() {
	for _, w := range c.workers {
		w.Close()
	}
	c.srv.Close()
	<-c.runDone
	for range c.workers {
		<-c.exited
	}
	c.col.Stop()
}
