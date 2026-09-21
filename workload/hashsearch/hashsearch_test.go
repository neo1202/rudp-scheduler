package hashsearch

import (
	"math"
	"math/rand"
	"testing"
	"testing/quick"

	"github.com/neo1202/rudp-scheduler/workload"
)

var w Workload

func TestHashKnownAnswers(t *testing.T) {
	// printf 'hello 0' | shasum -a 256 | cut -c1-16
	if got, want := Hash("hello", 0), uint64(0xbca6e3867c18225d); got != want {
		t.Errorf("Hash(hello, 0) = %#x, want %#x", got, want)
	}
	if got, want := Hash("rudp", math.MaxUint64), uint64(0x9ad96e37bca30cea); got != want {
		t.Errorf("Hash(rudp, max) = %#x, want %#x", got, want)
	}
}

func TestComputeMatchesBruteForce(t *testing.T) {
	const lo, hi = 1000, 3000
	want := w.Zero()
	for n := uint64(lo); n < hi; n++ {
		if h := Hash("msg", n); h < want.Hash {
			want = workload.Partial{Hash: h, Nonce: n}
		}
	}
	if got := w.Compute("msg", lo, hi); got != want {
		t.Errorf("Compute = %+v, want %+v", got, want)
	}
	if got := w.Compute("msg", 5, 5); got != w.Zero() {
		t.Errorf("Compute over an empty range = %+v, want Zero", got)
	}
}

// The three algebraic properties below are what makes out-of-order,
// at-least-once delivery safe for this workload. They are checked twice: over
// the full 64-bit domain, and over a tiny domain where ties are common.

func small(h, n uint8) workload.Partial {
	return workload.Partial{Hash: uint64(h % 3), Nonce: uint64(n % 3)}
}

func check(t *testing.T, name string, prop func(a, b, c workload.Partial) bool) {
	t.Helper()
	full := func(a, b, c workload.Partial) bool { return prop(a, b, c) }
	if err := quick.Check(full, &quick.Config{MaxCount: 2000}); err != nil {
		t.Errorf("%s (full domain): %v", name, err)
	}
	tiny := func(ah, an, bh, bn, ch, cn uint8) bool {
		return prop(small(ah, an), small(bh, bn), small(ch, cn))
	}
	if err := quick.Check(tiny, &quick.Config{MaxCount: 5000}); err != nil {
		t.Errorf("%s (tie-heavy domain): %v", name, err)
	}
}

func TestMergeIsCommutative(t *testing.T) {
	check(t, "commutative", func(a, b, _ workload.Partial) bool {
		return w.Merge(a, b) == w.Merge(b, a)
	})
}

func TestMergeIsAssociative(t *testing.T) {
	check(t, "associative", func(a, b, c workload.Partial) bool {
		return w.Merge(w.Merge(a, b), c) == w.Merge(a, w.Merge(b, c))
	})
}

func TestMergeIsIdempotent(t *testing.T) {
	check(t, "idempotent", func(a, b, _ workload.Partial) bool {
		ab := w.Merge(a, b)
		return w.Merge(a, a) == a && w.Merge(ab, b) == ab && w.Merge(ab, a) == ab
	})
}

func TestZeroIsIdentity(t *testing.T) {
	check(t, "identity", func(a, _, _ workload.Partial) bool {
		return w.Merge(w.Zero(), a) == a && w.Merge(a, w.Zero()) == a
	})
}

// The consequence the scheduler relies on: fold the chunk results in any
// order, with any of them repeated any number of times, and the answer equals
// the sequential scan of the whole range.
func TestAnyOrderAnyRepetitionGivesSequentialAnswer(t *testing.T) {
	const lo, hi, chunk = 0, 4000, 250
	want := w.Compute("job", lo, hi)
	var parts []workload.Partial
	for s := uint64(lo); s < hi; s += chunk {
		parts = append(parts, w.Compute("job", s, s+chunk))
	}

	prop := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		seq := append([]workload.Partial(nil), parts...)
		for i := 0; i < len(parts); i++ { // duplicate some at random
			if rng.Intn(2) == 0 {
				seq = append(seq, parts[rng.Intn(len(parts))])
			}
		}
		rng.Shuffle(len(seq), func(i, j int) { seq[i], seq[j] = seq[j], seq[i] })
		acc := w.Zero()
		for _, p := range seq {
			acc = w.Merge(acc, p)
		}
		return acc == want
	}
	if err := quick.Check(prop, &quick.Config{MaxCount: 500}); err != nil {
		t.Error(err)
	}
}

func BenchmarkCompute10k(b *testing.B) {
	for i := 0; i < b.N; i++ {
		w.Compute("benchmark message", 0, 10000)
	}
}
