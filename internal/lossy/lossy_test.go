package lossy

import (
	"encoding/binary"
	"errors"
	"math"
	"net"
	"sync"
	"testing"
	"time"
)

// recorder is an in-memory PacketConn that remembers what was written to it.
type recorder struct {
	net.PacketConn
	mu     sync.Mutex
	pkts   []uint32
	closed bool
}

func (r *recorder) WriteTo(b []byte, _ net.Addr) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pkts = append(r.pkts, binary.BigEndian.Uint32(b))
	return len(b), nil
}

func (r *recorder) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	return nil
}

func (r *recorder) snapshot() []uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uint32(nil), r.pkts...)
}

func writeN(c *Conn, n int) {
	var b [4]byte
	for i := 0; i < n; i++ {
		binary.BigEndian.PutUint32(b[:], uint32(i))
		c.WriteTo(b[:], nil)
	}
}

func TestRatesConverge(t *testing.T) {
	const n = 50000
	rec := &recorder{}
	c := Wrap(rec, Config{Drop: 0.2, Dup: 0.1, Seed: 1})
	writeN(c, n)
	// WriteTo only queues; wait until the owner goroutine has seen everything.
	st := c.Stats()
	for deadline := time.Now().Add(5 * time.Second); st.Written < n && time.Now().Before(deadline); st = c.Stats() {
		time.Sleep(time.Millisecond)
	}
	c.Close()

	if st.Written != n {
		t.Fatalf("Written = %d, want %d", st.Written, n)
	}
	dropRate := float64(st.Dropped) / n
	if math.Abs(dropRate-0.2) > 0.01 {
		t.Errorf("drop rate = %.3f, want 0.20 +- 0.01", dropRate)
	}
	dupRate := float64(st.Duplicated) / float64(n-int(st.Dropped))
	if math.Abs(dupRate-0.1) > 0.01 {
		t.Errorf("dup rate = %.3f, want 0.10 +- 0.01", dupRate)
	}
	if got, want := len(rec.snapshot()), n-int(st.Dropped)+int(st.Duplicated); got != want {
		t.Errorf("inner saw %d packets, want %d", got, want)
	}
}

func TestSameSeedSameDecisions(t *testing.T) {
	run := func(seed int64) []uint32 {
		rec := &recorder{}
		c := Wrap(rec, Config{Drop: 0.3, Dup: 0.2, Seed: seed})
		writeN(c, 2000)
		c.Close()
		return rec.snapshot()
	}
	a, b, other := run(42), run(42), run(43)
	if len(a) != len(b) {
		t.Fatalf("same seed produced %d and %d packets", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("same seed diverged at %d: %d vs %d", i, a[i], b[i])
		}
	}
	same := len(a) == len(other)
	for i := 0; same && i < len(a); i++ {
		same = a[i] == other[i]
	}
	if same {
		t.Error("different seeds produced identical decisions")
	}
}

func TestFilterDropsChosenPacket(t *testing.T) {
	rec := &recorder{}
	seen := false
	c := Wrap(rec, Config{Filter: func(b []byte) bool {
		// Drop only the first time packet 7 shows up.
		if binary.BigEndian.Uint32(b) == 7 && !seen {
			seen = true
			return true
		}
		return false
	}})
	writeN(c, 10)
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], 7)
	c.WriteTo(b[:], nil)
	c.Close()

	got := rec.snapshot()
	want := []uint32{0, 1, 2, 3, 4, 5, 6, 8, 9, 7}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestJitterReordersAndCloseFlushes(t *testing.T) {
	rec := &recorder{}
	c := Wrap(rec, Config{Jitter: 30 * time.Millisecond, Seed: 7})
	writeN(c, 200)

	time.Sleep(60 * time.Millisecond)
	got := rec.snapshot()
	if len(got) != 200 {
		t.Fatalf("after the jitter window inner saw %d packets, want 200", len(got))
	}
	inOrder := true
	for i := 1; i < len(got); i++ {
		if got[i] < got[i-1] {
			inOrder = false
			break
		}
	}
	if inOrder {
		t.Error("jitter did not reorder anything")
	}

	// Anything still parked when Close is called must go out, not vanish.
	writeN(c, 50)
	c.Close()
	if n := len(rec.snapshot()); n != 250 {
		t.Errorf("after Close inner saw %d packets, want 250", n)
	}
	if !rec.closed {
		t.Error("inner connection was not closed")
	}
}

func TestWriteAfterClose(t *testing.T) {
	c := Wrap(&recorder{}, Config{})
	c.Close()
	c.Close() // idempotent
	if _, err := c.WriteTo([]byte{0, 0, 0, 0}, nil); !errors.Is(err, net.ErrClosed) {
		t.Errorf("WriteTo after Close: err = %v, want net.ErrClosed", err)
	}
	if st := c.Stats(); st != (Stats{}) {
		t.Errorf("Stats after Close = %+v, want zero", st)
	}
}
