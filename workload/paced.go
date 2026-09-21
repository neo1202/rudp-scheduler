package workload

import "time"

// Paced wraps a Workload and controls how long each Compute call takes on the
// wall clock. Tests, benchmarks and demos use it to model a worker with a
// fixed per-chunk cost, or one that degrades part-way through a job.
//
// A Paced value counts its calls and must therefore be driven by a single
// goroutine, which is how a worker uses its Workload anyway.
type Paced struct {
	Inner Workload

	// MinCost pads every Compute call to at least this duration.
	MinCost time.Duration

	// After SlowAfter calls, every call is stretched to Slowdown times its
	// (padded) duration. Slowdown <= 1 disables the effect.
	Slowdown  float64
	SlowAfter int

	calls int
}

var _ Workload = (*Paced)(nil)

// Compute runs the inner workload, then sleeps off the rest of the target
// duration.
func (p *Paced) Compute(msg string, lo, hi uint64) Partial {
	start := time.Now()
	out := p.Inner.Compute(msg, lo, hi)
	p.calls++

	target := time.Since(start)
	if target < p.MinCost {
		target = p.MinCost
	}
	if p.Slowdown > 1 && p.calls > p.SlowAfter {
		target = time.Duration(float64(target) * p.Slowdown)
	}
	time.Sleep(target - time.Since(start))
	return out
}

// Merge delegates to the inner workload.
func (p *Paced) Merge(a, b Partial) Partial { return p.Inner.Merge(a, b) }

// Zero delegates to the inner workload.
func (p *Paced) Zero() Partial { return p.Inner.Zero() }
