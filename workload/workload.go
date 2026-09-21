// Package workload defines what the scheduler needs to know about a job: how
// to evaluate one slice of it, and how to combine two partial answers.
//
// The scheduler runs on a transport that delivers messages out of order and
// sometimes more than once, and it re-executes chunks on purpose (speculation,
// worker failure). That is only sound if combining partial answers does not
// care about order or repetition, so the contract on Merge is strict:
//
//	Merge(a, b) == Merge(b, a)                      commutative
//	Merge(Merge(a, b), c) == Merge(a, Merge(b, c))  associative
//	Merge(a, a) == a                                idempotent
//	Merge(Zero(), a) == a                           identity
//
// Any job that splits into independent ranges and folds with such a Merge
// fits: minimum or maximum searches (hash search, parameter sweeps, fuzzing
// for the smallest crashing input), set unions, and so on.
package workload

// Partial is the answer for one range of a job. On the wire it is a fixed
// pair of 64-bit values. For hash search they are the smallest hash seen and
// the nonce that produced it; other workloads map their own "score" and
// "argument" onto the same two fields.
type Partial struct {
	Hash  uint64
	Nonce uint64
}

// Workload is one kind of job.
type Workload interface {
	// Compute evaluates the half-open range [lo, hi) of the job identified
	// by msg. It must be a pure function of its arguments.
	Compute(msg string, lo, hi uint64) Partial

	// Merge combines two partial answers. It MUST be commutative,
	// associative and idempotent, with Zero as its identity.
	Merge(a, b Partial) Partial

	// Zero is the answer for an empty range.
	Zero() Partial
}
