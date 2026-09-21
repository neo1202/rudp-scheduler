// Package hashsearch is the default workload: a proof-of-work style brute
// force search for the nonce in [lo, hi) that minimises a SHA-256 based hash.
//
// It is the cleanest illustration of what the scheduler assumes about a job.
// The work is CPU bound, chunks share nothing, and the merge is a minimum,
// which is commutative, associative and idempotent. So it does not matter in
// which order chunk results arrive, or how many times the same chunk is
// computed: the answer is the same.
package hashsearch

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
	"strconv"

	"github.com/neo1202/rudp-scheduler/workload"
)

// Workload implements workload.Workload. The zero value is ready to use.
type Workload struct{}

var _ workload.Workload = Workload{}

// Hash returns the first 8 bytes, big-endian, of
// SHA-256(msg + " " + nonce in decimal).
func Hash(msg string, nonce uint64) uint64 {
	buf := make([]byte, 0, len(msg)+21)
	buf = append(buf, msg...)
	buf = append(buf, ' ')
	buf = strconv.AppendUint(buf, nonce, 10)
	sum := sha256.Sum256(buf)
	return binary.BigEndian.Uint64(sum[:8])
}

// Compute scans [lo, hi) and returns the smallest hash and its nonce. Ties go
// to the smaller nonce.
func (Workload) Compute(msg string, lo, hi uint64) workload.Partial {
	best := Workload{}.Zero()
	buf := make([]byte, 0, len(msg)+21)
	buf = append(buf, msg...)
	buf = append(buf, ' ')
	prefix := len(buf)
	for nonce := lo; nonce < hi; nonce++ {
		buf = strconv.AppendUint(buf[:prefix], nonce, 10)
		sum := sha256.Sum256(buf)
		h := binary.BigEndian.Uint64(sum[:8])
		if h < best.Hash || (h == best.Hash && nonce < best.Nonce) {
			best = workload.Partial{Hash: h, Nonce: nonce}
		}
	}
	return best
}

// Merge keeps the smaller hash; on equal hashes, the smaller nonce. This is
// the minimum under lexicographic (Hash, Nonce) order, hence commutative,
// associative and idempotent.
func (Workload) Merge(a, b workload.Partial) workload.Partial {
	if b.Hash < a.Hash || (b.Hash == a.Hash && b.Nonce < a.Nonce) {
		return b
	}
	return a
}

// Zero is the maximum of the (Hash, Nonce) order, the identity of Merge.
func (Workload) Zero() workload.Partial {
	return workload.Partial{Hash: math.MaxUint64, Nonce: math.MaxUint64}
}
