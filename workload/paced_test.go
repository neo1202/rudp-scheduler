package workload_test

import (
	"testing"
	"time"

	"github.com/neo1202/rudp-scheduler/workload"
	"github.com/neo1202/rudp-scheduler/workload/hashsearch"
)

func TestPacedStretchesComputeButNotTheAnswer(t *testing.T) {
	inner := hashsearch.Workload{}
	p := &workload.Paced{Inner: inner, MinCost: 2 * time.Millisecond, Slowdown: 10, SlowAfter: 2}
	want := inner.Compute("m", 0, 100)

	var took []time.Duration
	for i := 0; i < 4; i++ {
		start := time.Now()
		if got := p.Compute("m", 0, 100); got != want {
			t.Fatalf("call %d: answer changed: %+v != %+v", i, got, want)
		}
		took = append(took, time.Since(start))
	}
	for i, d := range took[:2] {
		if d < 2*time.Millisecond || d > 15*time.Millisecond {
			t.Errorf("call %d took %v, want about 2ms", i, d)
		}
	}
	for i, d := range took[2:] {
		if d < 20*time.Millisecond {
			t.Errorf("slow call %d took %v, want at least 20ms", i+2, d)
		}
	}
	if p.Merge(want, p.Zero()) != want {
		t.Error("Merge/Zero are not delegated")
	}
}
