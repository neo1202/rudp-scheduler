package sched

import (
	"flag"
	"time"

	"github.com/neo1202/rudp-scheduler/rudp"
	"github.com/neo1202/rudp-scheduler/wire"
)

// Params holds every tunable of a server: the transport's and the scheduler's.
// The zero value of any field means "use the default".
type Params struct {
	// Transport is handed to rudp.NewServer. Its Window doubles as the cap on
	// each worker's in-flight tasks, the one place where the scheduler looks
	// at a transport parameter.
	Transport rudp.Params

	ChunkSize       uint64  // nonces per task (default 10000)
	StragglerFactor float64 // a task is a straggler after this many task-level SRTTs (default 3)
	MinStragglerMs  int     // floor for the straggler threshold, in milliseconds (default 50)
	TickMs          int     // how often each worker scans its own in-flight tasks (default 10)
	NQCap           int     // capacity of the normal queue (default 4096)
	MaxWorkers      int     // sizes the priority queue: MaxWorkers * Window (default 64)
	NoSpeculate     bool    // disable speculative re-execution (benchmarks only)

	// WALPath, if set, makes jobs survive a server restart: the aggregator
	// logs every job and every counted chunk there and replays it on start.
	WALPath string
	// JobGraceMs is how long a job outlives its client's connection, waiting
	// for the client to come back with the same job ID (default 5000).
	JobGraceMs int
	// ResultTTLMs is how long a finished job's answer is kept for a client
	// that asks again (default 60000).
	ResultTTLMs int

	// WorkerStats and AggStats, if set, receive cumulative snapshots.
	// Sends are non-blocking; a full channel drops a snapshot.
	WorkerStats chan<- WorkerStats
	AggStats    chan<- AggStats

	// Hooks are test probes, called on the goroutine that owns the state.
	Hooks *Hooks
}

// Default values, from the design's parameter table.
const (
	DefaultChunkSize       = 10000
	DefaultStragglerFactor = 3
	DefaultMinStragglerMs  = 50
	DefaultTickMs          = 10
	DefaultNQCap           = 4096
	DefaultMaxWorkers      = 64
	DefaultJobGraceMs      = 5000
	DefaultResultTTLMs     = 60000

	// maxChunksPerJob bounds a job's task count, and with it the size of the
	// aggregator's bookkeeping. Larger ranges get proportionally larger chunks.
	maxChunksPerJob = 1 << 20
)

func (p *Params) withDefaults() *Params {
	var q Params
	if p != nil {
		q = *p
	}
	q.Transport = *q.Transport.WithDefaults()
	if q.ChunkSize == 0 {
		q.ChunkSize = DefaultChunkSize
	}
	if q.StragglerFactor <= 0 {
		q.StragglerFactor = DefaultStragglerFactor
	}
	if q.MinStragglerMs <= 0 {
		q.MinStragglerMs = DefaultMinStragglerMs
	}
	if q.TickMs <= 0 {
		q.TickMs = DefaultTickMs
	}
	if q.NQCap <= 0 {
		q.NQCap = DefaultNQCap
	}
	if q.MaxWorkers <= 0 {
		q.MaxWorkers = DefaultMaxWorkers
	}
	if q.JobGraceMs <= 0 {
		q.JobGraceMs = DefaultJobGraceMs
	}
	if q.ResultTTLMs <= 0 {
		q.ResultTTLMs = DefaultResultTTLMs
	}
	return &q
}

// RegisterFlags binds every tunable, transport included, to fs.
func (p *Params) RegisterFlags(fs *flag.FlagSet) {
	p.Transport.RegisterFlags(fs)
	fs.Uint64Var(&p.ChunkSize, "chunk-size", DefaultChunkSize, "nonces per task")
	fs.Float64Var(&p.StragglerFactor, "straggler-factor", DefaultStragglerFactor, "speculate on a task older than this many task-level SRTTs")
	fs.IntVar(&p.MinStragglerMs, "min-straggler-ms", DefaultMinStragglerMs, "floor for the straggler threshold, in milliseconds")
	fs.IntVar(&p.TickMs, "tick-ms", DefaultTickMs, "how often each worker scans its in-flight tasks, in milliseconds")
	fs.IntVar(&p.NQCap, "nq-cap", DefaultNQCap, "capacity of the normal task queue")
	fs.IntVar(&p.MaxWorkers, "max-workers", DefaultMaxWorkers, "sizes the priority queue (max-workers * window)")
	fs.BoolVar(&p.NoSpeculate, "no-speculate", false, "disable speculative re-execution of stragglers")
	fs.StringVar(&p.WALPath, "wal", "", "write-ahead log file; jobs survive a server restart when set")
	fs.IntVar(&p.JobGraceMs, "job-grace-ms", DefaultJobGraceMs, "how long a job waits for a disconnected client to come back")
	fs.IntVar(&p.ResultTTLMs, "result-ttl-ms", DefaultResultTTLMs, "how long a finished job's answer is kept for re-requests")
}

// Hooks lets tests observe the scheduler without sharing its state. Each hook
// runs on the goroutine that owns the state it reports on.
type Hooks struct {
	OnSend      func(worker int, id wire.TaskID, inflight int) // workerLoop: task recorded and written
	OnSpeculate func(worker int, id wire.TaskID)               // workerLoop: clone pushed to pq
	OnMerge     func(id wire.TaskID)                           // aggregator: result counted
	OnDiscard   func(id wire.TaskID, duplicate bool)           // aggregator: result dropped (duplicate, or job gone)
	OnJobEnd    func(job uint64, completed bool)               // aggregator: job finished or abandoned
}

// WorkerStats is a cumulative snapshot published by one workerLoop.
type WorkerStats struct {
	Worker     int
	Inflight   int
	SRTT       time.Duration // task-level smoothed round trip
	Sent       uint64        // chunks handed to this worker
	Results    uint64        // results accepted from it
	DupResults uint64        // results for tasks no longer in flight (transport duplicates)
	Speculated uint64        // clones this worker pushed to pq
	Requeued   uint64        // tasks handed back when the worker was lost
	Busy       time.Duration // time with at least one task in flight
	Alive      time.Duration // time since the worker joined
	Gone       bool          // final snapshot
}

// LatencyBounds are the upper bounds, in seconds, of the job latency histogram.
var LatencyBounds = [...]float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}

// Histogram is a fixed-bucket histogram. Buckets[i] counts observations at or
// below LatencyBounds[i]; the last bucket is +Inf. Buckets are not cumulative.
type Histogram struct {
	Buckets [len(LatencyBounds) + 1]uint64
	Sum     float64
	Count   uint64
}

func (h *Histogram) observe(seconds float64) {
	i := 0
	for i < len(LatencyBounds) && seconds > LatencyBounds[i] {
		i++
	}
	h.Buckets[i]++
	h.Sum += seconds
	h.Count++
}

// AggStats is a cumulative snapshot published by the aggregator.
type AggStats struct {
	JobsStarted     uint64
	JobsCompleted   uint64
	JobsDropped     uint64 // client went away and did not return within the grace period
	JobsRecovered   uint64 // running jobs rebuilt from the write-ahead log at start
	ChunksRecovered uint64 // counted chunks rebuilt from the log (work not redone)
	Reattached      uint64 // requests that joined a running job or fetched a stored answer
	WALErrors       uint64 // failed log writes; the scheduler carries on in memory
	ActiveJobs      int
	Merged          uint64 // results counted, exactly one per TaskID
	Duplicates      uint64 // results dropped because their TaskID was already counted
	Orphans         uint64 // results dropped because their job no longer exists
	JobLatency      Histogram
}
