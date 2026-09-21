package main

import (
	"fmt"
	"log"
	"math"
	"os/exec"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/neo1202/rudp-scheduler/rudp"
	"github.com/neo1202/rudp-scheduler/sched"
	"github.com/neo1202/rudp-scheduler/workload"
	"github.com/neo1202/rudp-scheduler/workload/hashsearch"
)

// sizes fixes how much work every experiment does.
type sizes struct {
	reps, jobs int    // clusters per configuration, measured jobs per cluster
	smallChunk uint64 // the scheduler's default chunk size
	largeChunk uint64 // a chunk that costs a few milliseconds on this class of CPU
	scalingN   uint64 // nonces per job in the scaling experiment
	jobChunks  uint64 // chunks per job in the other experiments (at largeChunk)
	workerSet  []int
	lossSet    []float64
}

var fullSizes = sizes{
	reps: 5, jobs: 8,
	smallChunk: sched.DefaultChunkSize, largeChunk: 100_000,
	scalingN: 40_000_000, jobChunks: 400,
	workerSet: []int{1, 2, 4, 8},
	lossSet:   []float64{0, 0.05, 0.10, 0.20},
}

var quickSizes = sizes{
	reps: 1, jobs: 2,
	smallChunk: 2_000, largeChunk: 5_000,
	scalingN: 200_000, jobChunks: 40,
	workerSet: []int{1, 2},
	lossSet:   []float64{0, 0.10},
}

const benchMsg = "benchmark"

// measurement is what one configuration produced, over all its clusters.
type measurement struct {
	times                   []time.Duration // one per measured job
	nonces                  uint64          // per job
	busy                    time.Duration   // worker compute time during measured jobs
	workers                 int
	firstSends, retransmits uint64 // transport, all endpoints, measured jobs only
	dispatched, merged      uint64 // scheduler: chunks handed out vs chunks counted
	speculated, duplicates  uint64
	failed                  int
}

var expected = map[uint64]workload.Partial{}

// answer computes the reference result for [0, hi) once, using every core.
func answer(hi uint64) workload.Partial {
	if p, ok := expected[hi]; ok {
		return p
	}
	var hs hashsearch.Workload
	parts := uint64(runtime.NumCPU())
	ch := make(chan workload.Partial, parts)
	for i := uint64(0); i < parts; i++ {
		go func() { ch <- hs.Compute(benchMsg, hi/parts*i, hi/parts*(i+1)) }()
	}
	acc := hs.Compute(benchMsg, hi/parts*parts, hi)
	for i := uint64(0); i < parts; i++ {
		acc = hs.Merge(acc, <-ch)
	}
	expected[hi] = acc
	return acc
}

// measure runs cfg on sz.reps fresh clusters. Each cluster first runs one
// unmeasured warm-up job, then sz.jobs measured jobs back to back, every one
// checked against the sequential answer.
func measure(cfg config, sz sizes, nonces uint64, baseSeed int64) measurement {
	m := measurement{nonces: nonces, workers: cfg.workers}
	want := answer(nonces)
	for rep := 0; rep < sz.reps; rep++ {
		cfg.seed = baseSeed + int64(rep)*1000
		c := startCluster(cfg)

		if _, _, err := c.job(benchMsg, 0, nonces); err != nil {
			log.Fatalf("warm-up job failed: %v", err)
		}
		c.drainBusy()
		before := c.snapshot()

		for j := 0; j < sz.jobs; j++ {
			got, took, err := c.job(benchMsg, 0, nonces)
			switch {
			case err != nil:
				m.failed++
			case got != want:
				log.Fatalf("wrong answer: got %+v, want %+v (config %+v)", got, want, cfg)
			default:
				m.times = append(m.times, took)
				m.busy += c.drainBusy()
			}
		}

		after := c.snapshot()
		m.firstSends += after.DataSent() - before.DataSent()
		m.retransmits += after.Retransmits() - before.Retransmits()
		m.dispatched += after.WorkerTotals().Sent - before.WorkerTotals().Sent
		m.speculated += after.WorkerTotals().Speculated - before.WorkerTotals().Speculated
		m.merged += after.Agg.Merged - before.Agg.Merged
		m.duplicates += after.Agg.Duplicates - before.Agg.Duplicates
		c.close()
	}
	return m
}

func (m measurement) total() time.Duration {
	var t time.Duration
	for _, d := range m.times {
		t += d
	}
	return t
}

// throughput returns the mean and standard deviation, over jobs, of millions
// of nonces searched per second.
func (m measurement) throughput() (mean, sd float64) {
	var xs []float64
	for _, d := range m.times {
		xs = append(xs, float64(m.nonces)/d.Seconds()/1e6)
	}
	return meanSD(xs)
}

// utilization is the fraction of the measured jobs' wall time that the
// workers spent inside Compute.
func (m measurement) utilization() float64 {
	if t := m.total(); t > 0 {
		return m.busy.Seconds() / (t.Seconds() * float64(m.workers))
	}
	return 0
}

func (m measurement) percentile(p float64) time.Duration {
	if len(m.times) == 0 {
		return 0
	}
	s := append([]time.Duration(nil), m.times...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	rank := int(math.Ceil(p/100*float64(len(s)))) - 1 // nearest rank
	if rank < 0 {
		rank = 0
	}
	return s[rank]
}

func meanSD(xs []float64) (mean, sd float64) {
	if len(xs) == 0 {
		return 0, 0
	}
	for _, x := range xs {
		mean += x
	}
	mean /= float64(len(xs))
	for _, x := range xs {
		sd += (x - mean) * (x - mean)
	}
	if len(xs) > 1 {
		sd = math.Sqrt(sd / float64(len(xs)-1))
	}
	return mean, sd
}

func ms(d time.Duration) string { return fmt.Sprintf("%.0f ms", float64(d.Microseconds())/1000) }
func pct(x float64) string      { return fmt.Sprintf("%.1f%%", 100*x) }

func ratio(a, b uint64) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}

// extraWork is the share of chunk hand-outs beyond one per counted task.
func extraWork(m measurement) float64 {
	if m.merged == 0 || m.dispatched <= m.merged {
		return 0
	}
	return float64(m.dispatched-m.merged) / float64(m.merged)
}

func failures(ms ...measurement) string {
	n := 0
	for _, m := range ms {
		n += m.failed
	}
	if n == 0 {
		return ""
	}
	return fmt.Sprintf("\n%d job(s) ended with a lost connection and are excluded.\n", n)
}

func writeHeader(b *strings.Builder, o options, sz sizes) {
	d := rudp.DefaultParams()
	fmt.Fprintf(b, "# Benchmarks\n\n")
	if o.quick {
		fmt.Fprintf(b, "> **Smoke-test run (`-quick`). The numbers below are meaningless.**\n\n")
	}
	fmt.Fprintf(b, "Generated by `go run ./cmd/bench` (seed %d). Do not edit by hand; rerun instead.\n\n", o.seed)
	fmt.Fprintf(b, "## Setup\n\n")
	fmt.Fprintf(b, "| | |\n|---|---|\n")
	fmt.Fprintf(b, "| Machine | %s, %d logical CPUs, %s/%s |\n", cpuModel(), runtime.NumCPU(), runtime.GOOS, runtime.GOARCH)
	fmt.Fprintf(b, "| Go | %s, GOMAXPROCS %d |\n", runtime.Version(), runtime.GOMAXPROCS(0))
	fmt.Fprintf(b, "| Topology | one process: 1 server, N workers, 1 client per job, all on real UDP sockets over loopback |\n")
	fmt.Fprintf(b, "| Loss model | `internal/lossy` on every socket's send path, so a loss rate of p applies independently to each direction; seeded, no duplication or jitter |\n")
	fmt.Fprintf(b, "| Transport | epoch %d ms, window %d, max backoff %d epochs, epochLimit %d (default %d, raised so that lost heartbeats cannot end an idle client connection at 20%% loss) |\n",
		d.EpochMs, d.Window, d.MaxBackoff, benchEpochLimit, d.EpochLimit)
	fmt.Fprintf(b, "| Scheduler | straggler threshold max(%d x task SRTT, %d ms), tick %d ms |\n",
		sched.DefaultStragglerFactor, sched.DefaultMinStragglerMs, sched.DefaultTickMs)
	fmt.Fprintf(b, "| Workload | hash search, SHA-256; a chunk of %d nonces costs about %s on one core of this machine, a chunk of %d about %s |\n",
		sz.smallChunk, chunkCost(sz.smallChunk), sz.largeChunk, chunkCost(sz.largeChunk))
	fmt.Fprintf(b, "| Repetitions | every row: %d fresh clusters (different seeds) x %d measured jobs = %d jobs, after one unmeasured warm-up job per cluster; every answer is checked against the sequential result |\n\n",
		sz.reps, sz.jobs, sz.reps*sz.jobs)
	fmt.Fprintf(b, "Workers and server share one machine, so with 8 workers on %d cores the workers also compete\nwith the server and with each other for CPU. "+
		"Percentiles are nearest-rank over the jobs of a row; with %d samples p99 is the slowest job.\n\n",
		runtime.NumCPU(), sz.reps*sz.jobs)
}

func chunkCost(n uint64) string {
	var hs hashsearch.Workload
	hs.Compute(benchMsg, 0, n) // warm up
	const rounds = 20
	start := time.Now()
	for i := 0; i < rounds; i++ {
		hs.Compute(benchMsg, 0, n)
	}
	return fmt.Sprintf("%.2f ms", float64(time.Since(start).Microseconds())/1000/rounds)
}

func cpuModel() string {
	if out, err := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output(); err == nil {
		model := strings.TrimSpace(string(out))
		// Apple silicon: say how the cores split, it matters for scaling.
		if lv, err := exec.Command("sysctl", "-n", "hw.perflevel0.physicalcpu", "hw.perflevel1.physicalcpu").Output(); err == nil {
			if f := strings.Fields(string(lv)); len(f) == 2 {
				model += fmt.Sprintf(" (%s performance + %s efficiency cores)", f[0], f[1])
			}
		}
		return model
	}
	if out, err := exec.Command("sh", "-c", "grep -m1 'model name' /proc/cpuinfo | cut -d: -f2").Output(); err == nil && len(out) > 1 {
		return strings.TrimSpace(string(out))
	}
	return "unknown CPU"
}

// Experiment 1: throughput and scaling efficiency with 1, 2, 4 and 8 workers.
func scaling(b *strings.Builder, o options, sz sizes) {
	fmt.Fprintf(b, "## 1. Scaling\n\n")
	fmt.Fprintf(b, "One job of %d nonces. Throughput is nonces searched per second of client-observed job time, mean and\n"+
		"standard deviation over the jobs of the row. Efficiency is throughput divided by (workers x the 1-worker\nthroughput of the same table). "+
		"Utilization is the share of job time the workers spent computing.\n\n", sz.scalingN)
	for _, chunk := range []uint64{sz.smallChunk, sz.largeChunk} {
		for _, drop := range []float64{0, 0.10} {
			fmt.Fprintf(b, "**Chunk = %d nonces, loss = %s**\n\n", chunk, pct(drop))
			fmt.Fprintf(b, "| Workers | Throughput (M nonces/s) | Speedup | Efficiency | Worker utilization | Retransmit rate | Jobs |\n|---|---|---|---|---|---|---|\n")
			var base float64
			var all []measurement
			for _, w := range sz.workerSet {
				log.Printf("scaling: chunk=%d loss=%s workers=%d", chunk, pct(drop), w)
				m := measure(config{workers: w, drop: drop, chunkSize: chunk}, sz, sz.scalingN, o.seed)
				all = append(all, m)
				mean, sd := m.throughput()
				if base == 0 {
					base = mean / float64(w)
				}
				fmt.Fprintf(b, "| %d | %.1f ± %.1f | %.2fx | %s | %s | %s | %d |\n",
					w, mean, sd, mean/base, pct(mean/base/float64(w)), pct(m.utilization()), pct(ratio(m.retransmits, m.firstSends)), len(m.times))
			}
			fmt.Fprintf(b, "%s\n", failures(all...))
		}
	}
}

// Experiment 2: what head-of-line blocking would cost.
func headOfLine(b *strings.Builder, o options, sz sizes) {
	const workers = 4
	nonces := sz.jobChunks * sz.largeChunk
	fmt.Fprintf(b, "## 2. Head-of-line blocking\n\n")
	fmt.Fprintf(b, "%d workers, 10%% loss, chunk = %d nonces, %d chunks per job. The only difference between the rows is\n"+
		"`Params.OrderedDelivery`: the default hands every message up on arrival; the benchmark-only ordered mode\n"+
		"holds a message back until every earlier sequence number has been delivered, like a TCP-style transport.\n\n",
		workers, sz.largeChunk, sz.jobChunks)
	fmt.Fprintf(b, "| Delivery | Worker utilization | Job time p50 | Job time p99 | Throughput (M nonces/s) | Jobs |\n|---|---|---|---|---|---|\n")
	var all []measurement
	for _, ordered := range []bool{false, true} {
		log.Printf("hol: ordered=%v", ordered)
		m := measure(config{workers: workers, drop: 0.10, chunkSize: sz.largeChunk, ordered: ordered}, sz, nonces, o.seed)
		all = append(all, m)
		name := "on arrival (default)"
		if ordered {
			name = "in sequence order"
		}
		mean, sd := m.throughput()
		fmt.Fprintf(b, "| %s | %s | %s | %s | %.1f ± %.1f | %d |\n",
			name, pct(m.utilization()), ms(m.percentile(50)), ms(m.percentile(99)), mean, sd, len(m.times))
	}
	fmt.Fprintf(b, "%s\n", failures(all...))
}

// Experiment 3: what speculative re-execution buys when one worker degrades.
func hedging(b *strings.Builder, o options, sz sizes) {
	const workers = 4
	nonces := sz.jobChunks * sz.largeChunk
	fmt.Fprintf(b, "## 3. Hedging against a straggler\n\n")
	fmt.Fprintf(b, "%d workers, no loss, chunk = %d nonces, %d chunks per job. One worker runs at full speed for its first %d\n"+
		"chunks and then takes %dx as long for every chunk (it sleeps after computing), which happens during the\n"+
		"warm-up job; every measured job therefore runs with one degraded worker. "+
		"Extra work is chunks handed to\nworkers beyond one per task, as a share of the tasks.\n\n",
		workers, sz.largeChunk, sz.jobChunks, stragglerAfter, stragglerFactor)
	fmt.Fprintf(b, "| Speculation | Job time p50 | Job time p99 | Clones issued per job | Extra work | Jobs |\n|---|---|---|---|---|---|\n")
	var all []measurement
	for _, off := range []bool{false, true} {
		log.Printf("hedging: noSpeculate=%v", off)
		m := measure(config{workers: workers, chunkSize: sz.largeChunk, noSpeculate: off, straggler: true}, sz, nonces, o.seed)
		all = append(all, m)
		name := "on (default)"
		if off {
			name = "off"
		}
		jobs := float64(len(m.times))
		fmt.Fprintf(b, "| %s | %s | %s | %.1f | %s | %d |\n",
			name, ms(m.percentile(50)), ms(m.percentile(99)), float64(m.speculated)/jobs,
			pct(extraWork(m)), len(m.times))
	}
	fmt.Fprintf(b, "%s\n", failures(all...))
}

// Experiment 4: goodput and retransmission rate as loss grows.
func lossSweep(b *strings.Builder, o options, sz sizes) {
	const workers = 4
	nonces := sz.jobChunks * sz.largeChunk
	fmt.Fprintf(b, "## 4. Loss sweep\n\n")
	fmt.Fprintf(b, "%d workers, chunk = %d nonces, %d chunks per job. Goodput is useful work: nonces of completed jobs per\n"+
		"second. Retransmit rate is retransmissions divided by first transmissions, over every connection and both\n"+
		"endpoints. Clones are tasks the scheduler re-issued because a loss made them look like stragglers.\n\n",
		workers, sz.largeChunk, sz.jobChunks)
	fmt.Fprintf(b, "| Loss (each direction) | Goodput (M nonces/s) | vs. no loss | Worker utilization | Retransmit rate | Clones issued per job | Job time p50 | Job time p99 | Jobs |\n|---|---|---|---|---|---|---|---|---|\n")
	var base float64
	var all []measurement
	for _, drop := range sz.lossSet {
		log.Printf("loss: drop=%s", pct(drop))
		m := measure(config{workers: workers, drop: drop, chunkSize: sz.largeChunk}, sz, nonces, o.seed)
		all = append(all, m)
		mean, sd := m.throughput()
		if base == 0 {
			base = mean
		}
		fmt.Fprintf(b, "| %s | %.1f ± %.1f | %s | %s | %s | %.1f | %s | %s | %d |\n",
			pct(drop), mean, sd, pct(mean/base), pct(m.utilization()), pct(ratio(m.retransmits, m.firstSends)),
			float64(m.speculated)/float64(len(m.times)), ms(m.percentile(50)), ms(m.percentile(99)), len(m.times))
	}
	fmt.Fprintf(b, "%s\n", failures(all...))
}
