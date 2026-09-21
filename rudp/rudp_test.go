package rudp

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/neo1202/rudp-scheduler/internal/lossy"
)

// leakCheck fails the test if goroutines started during it are still running
// a few seconds after it finished.
func leakCheck(t *testing.T) {
	t.Helper()
	before := runtime.NumGoroutine()
	t.Cleanup(func() {
		deadline := time.Now().Add(5 * time.Second)
		for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if n := runtime.NumGoroutine(); n > before {
			buf := make([]byte, 1<<20)
			t.Errorf("goroutine leak: %d before, %d after\n%s", before, n, buf[:runtime.Stack(buf, true)])
		}
	})
}

// patient is a parameter set that will not declare a connection lost just
// because a loaded CI machine or a run of bad luck silenced it for a moment.
func patient() *Params { return &Params{EpochMs: 10, EpochLimit: 100} }

func lossyWrap(cfg lossy.Config) func(net.PacketConn) net.PacketConn {
	return func(pc net.PacketConn) net.PacketConn { return lossy.Wrap(pc, cfg) }
}

func with(p *Params, wrap func(net.PacketConn) net.PacketConn) *Params {
	q := *p
	q.WrapSocket = wrap
	return &q
}

func mustServer(t *testing.T, p *Params) *Server {
	t.Helper()
	s, err := NewServer(0, p)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func mustClient(t *testing.T, s *Server, p *Params) *Client {
	t.Helper()
	c, err := NewClient(fmt.Sprintf("127.0.0.1:%d", s.Port()), p)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func msg(i int) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(i))
	return b[:]
}

func idx(b []byte) int { return int(binary.BigEndian.Uint32(b)) }

// Acceptance: at 20% drop + 10% dup, 1000 messages in each direction are all
// delivered at least once. Duplicates are expected to reach the application,
// because the transport does not de-duplicate.
func TestAtLeastOnceUnderLoss(t *testing.T) {
	leakCheck(t)
	const n = 1000
	netCfg := func(seed int64) lossy.Config {
		return lossy.Config{Drop: 0.20, Dup: 0.10, Jitter: 2 * time.Millisecond, Seed: seed}
	}
	s := mustServer(t, with(patient(), lossyWrap(netCfg(1))))
	defer s.Close()
	c := mustClient(t, s, with(patient(), lossyWrap(netCfg(2))))
	defer c.Close()

	type tally struct{ distinct, total int }
	serverGot, clientGot := make(chan tally, 1), make(chan tally, 1)

	go func() { // server: echo nothing, but push n messages of its own
		seen := map[int]bool{}
		total, started := 0, false
		for len(seen) < n {
			h, data, err := s.Read()
			if err != nil {
				break
			}
			if !started {
				started = true
				for i := 0; i < n; i++ {
					h.Write(msg(i))
				}
			}
			seen[idx(data)] = true
			total++
		}
		serverGot <- tally{len(seen), total}
	}()
	go func() {
		seen := map[int]bool{}
		total := 0
		for len(seen) < n {
			data, err := c.Read()
			if err != nil {
				break
			}
			seen[idx(data)] = true
			total++
		}
		clientGot <- tally{len(seen), total}
	}()

	for i := 0; i < n; i++ {
		c.Write(msg(i))
	}

	dups := 0
	for name, ch := range map[string]chan tally{"server": serverGot, "client": clientGot} {
		select {
		case got := <-ch:
			if got.distinct != n {
				t.Errorf("%s received %d distinct messages, want %d", name, got.distinct, n)
			}
			dups += got.total - got.distinct
		case <-time.After(90 * time.Second):
			t.Fatalf("%s did not receive all %d messages in time", name, n)
		}
	}
	if dups == 0 {
		t.Error("no duplicate reached the application: is something de-duplicating below it?")
	}
	t.Logf("duplicates delivered to the application: %d", dups)
}

// dropFirstData returns a lossy filter that discards the first transmission
// of the Data packet with the given sequence number and nothing else.
func dropFirstData(seq uint32) func([]byte) bool {
	dropped := false
	return func(b []byte) bool {
		p, ok := parse(b)
		if ok && p.typ == typeData && p.seq == seq && !dropped {
			dropped = true
			return true
		}
		return false
	}
}

func readOrder(t *testing.T, s *Server, n int) []int {
	t.Helper()
	var order []int
	got := make(chan []int, 1)
	go func() {
		for len(order) < n {
			_, data, err := s.Read()
			if err != nil {
				break
			}
			order = append(order, idx(data))
		}
		got <- order
	}()
	select {
	case o := <-got:
		return o
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for deliveries")
		return nil
	}
}

// Acceptance: no head-of-line blocking. The first transmission of seq=3 is
// lost; messages behind it must reach the application before it does.
func TestNoHeadOfLineBlocking(t *testing.T) {
	leakCheck(t)
	s := mustServer(t, patient())
	defer s.Close()
	c := mustClient(t, s, with(patient(), lossyWrap(lossy.Config{Filter: dropFirstData(3)})))
	defer c.Close()

	const n = 10
	for i := 1; i <= n; i++ { // message i travels as seq i
		c.Write(msg(i))
	}
	order := readOrder(t, s, n)
	if len(order) != n {
		t.Fatalf("delivered %v, want %d messages", order, n)
	}
	pos := map[int]int{}
	for i, m := range order {
		pos[m] = i
	}
	for _, later := range []int{4, 5} { // same initial window as the lost packet
		if pos[later] > pos[3] {
			t.Errorf("message %d was held back behind the lost message 3: order %v", later, order)
		}
	}
	t.Logf("delivery order: %v", order)
}

// The benchmark-only ordered mode must do the opposite: strictly ascending,
// exactly once, even with duplicates on the wire.
func TestOrderedDeliveryHoldsBackBehindGap(t *testing.T) {
	leakCheck(t)
	sp := patient()
	sp.OrderedDelivery = true
	s := mustServer(t, sp)
	defer s.Close()
	c := mustClient(t, s, with(patient(), lossyWrap(lossy.Config{Dup: 0.5, Seed: 3, Filter: dropFirstData(3)})))
	defer c.Close()

	const n = 10
	for i := 1; i <= n; i++ {
		c.Write(msg(i))
	}
	order := readOrder(t, s, n)
	for i, m := range order {
		if m != i+1 {
			t.Fatalf("ordered mode delivered %v", order)
		}
	}
}

// Acceptance: an idle connection survives 50 epochs thanks to heartbeats, with
// default parameters (a connection is declared lost after only 5 silent epochs).
func TestIdleConnectionSurvives(t *testing.T) {
	leakCheck(t)
	p := DefaultParams()
	s := mustServer(t, p)
	defer s.Close()
	c := mustClient(t, s, p)
	defer c.Close()

	type event struct {
		who  string
		data []byte
		err  error
	}
	events := make(chan event, 4)
	go func() {
		_, data, err := s.Read()
		events <- event{"server", data, err}
	}()
	go func() {
		data, err := c.Read()
		events <- event{"client", data, err}
	}()

	select {
	case e := <-events:
		t.Fatalf("%s saw activity on an idle connection: data=%q err=%v", e.who, e.data, e.err)
	case <-time.After(50 * p.Epoch()):
	}

	c.Write([]byte("still here"))
	select {
	case e := <-events:
		if e.who != "server" || e.err != nil || string(e.data) != "still here" {
			t.Fatalf("after idling: %s got data=%q err=%v", e.who, e.data, e.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("connection did not carry data after idling")
	}
}

// rawSocket captures the real UDP socket so a test can pull the plug on an
// endpoint without giving it a chance to say goodbye.
func rawSocket(dst *net.PacketConn) func(net.PacketConn) net.PacketConn {
	return func(pc net.PacketConn) net.PacketConn { *dst = pc; return pc }
}

// Acceptance: when the peer vanishes, the loss is reported within EpochLimit
// epochs.
func TestVanishedPeerIsReported(t *testing.T) {
	p := &Params{EpochMs: 20, EpochLimit: 5}
	limit := time.Duration(p.EpochLimit) * p.Epoch()
	slack := 5 * p.Epoch() // scheduling noise on a loaded machine, not protocol time

	t.Run("client vanishes", func(t *testing.T) {
		leakCheck(t)
		s := mustServer(t, p)
		defer s.Close()
		var sock net.PacketConn
		c := mustClient(t, s, with(p, rawSocket(&sock)))
		defer c.Close()
		c.Write([]byte("hello"))
		if _, _, err := s.Read(); err != nil {
			t.Fatal(err)
		}

		sock.Close()
		start := time.Now()
		h, _, err := s.Read()
		elapsed := time.Since(start)
		if !errors.Is(err, ErrConnLost) || h == nil || h.ID() != c.ID() {
			t.Fatalf("Read = (%v, %v), want ErrConnLost for conn %d", h, err, c.ID())
		}
		if elapsed > limit+slack {
			t.Errorf("loss reported after %v, want within %v", elapsed, limit)
		}
		select {
		case <-h.Done():
		default:
			t.Error("handle.Done() not closed after the connection was lost")
		}
		t.Logf("reported after %v (limit %v)", elapsed, limit)
	})

	t.Run("server vanishes", func(t *testing.T) {
		leakCheck(t)
		var sock net.PacketConn
		s := mustServer(t, with(p, rawSocket(&sock)))
		defer s.Close()
		c := mustClient(t, s, p)
		defer c.Close()

		sock.Close()
		start := time.Now()
		_, err := c.Read()
		elapsed := time.Since(start)
		if !errors.Is(err, ErrConnLost) {
			t.Fatalf("Read err = %v, want ErrConnLost", err)
		}
		if elapsed > limit+slack {
			t.Errorf("loss reported after %v, want within %v", elapsed, limit)
		}
		if _, err := c.Read(); !errors.Is(err, ErrConnClosed) {
			t.Errorf("second Read err = %v, want ErrConnClosed", err)
		}
	})
}

// Acceptance: Write does not block when the window is full, and Close sends
// everything that was parked in pendingWrites.
func TestWriteNeverBlocksAndCloseFlushes(t *testing.T) {
	leakCheck(t)
	const n = 2000
	s := mustServer(t, patient())
	defer s.Close()

	var blackhole atomic.Bool // while set, every Data packet from the client is lost
	blackhole.Store(true)
	var maxBacklog atomic.Int64
	cp := with(patient(), lossyWrap(lossy.Config{Filter: func(b []byte) bool {
		p, ok := parse(b)
		return ok && p.typ == typeData && blackhole.Load()
	}}))
	cp.Hooks = &Hooks{OnBacklog: func(_ int, depth int) {
		if int64(depth) > maxBacklog.Load() {
			maxBacklog.Store(int64(depth))
		}
	}}
	c := mustClient(t, s, cp)

	start := time.Now()
	for i := 0; i < n; i++ {
		c.Write(msg(i))
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("%d Writes against a stuck window took %v: Write is blocking", n, d)
	}
	// Write returns as soon as mainLoop has taken the payload; give it a moment
	// to finish handling the last one.
	for deadline := time.Now().Add(time.Second); maxBacklog.Load() < n-DefaultWindow && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	if got, want := maxBacklog.Load(), int64(n-DefaultWindow); got != want {
		t.Errorf("pendingWrites peaked at %d, want %d (window stuck at %d)", got, want, DefaultWindow)
	}

	received := make(chan int, 1)
	go func() {
		seen := map[int]bool{}
		for {
			_, data, err := s.Read()
			if err != nil { // the client's Close arrives after its last ACK
				received <- len(seen)
				return
			}
			seen[idx(data)] = true
		}
	}()

	blackhole.Store(false)
	c.Close() // must not return before pendingWrites has been sent and acknowledged
	select {
	case got := <-received:
		if got != n {
			t.Errorf("server received %d distinct messages after Close, want %d", got, n)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("server never saw the connection close")
	}
}

func TestServerCloseFlushes(t *testing.T) {
	leakCheck(t)
	const n = 200
	s := mustServer(t, with(patient(), lossyWrap(lossy.Config{Drop: 0.2, Seed: 5})))
	c := mustClient(t, s, patient())
	defer c.Close()

	c.Write([]byte("hi"))
	h, _, err := s.Read()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		h.Write(msg(i))
	}
	closed := make(chan struct{})
	go func() { s.Close(); close(closed) }()

	seen := map[int]bool{}
	for {
		data, err := c.Read()
		if err != nil {
			break
		}
		seen[idx(data)] = true
	}
	if len(seen) != n {
		t.Errorf("client received %d distinct messages before the server closed, want %d", len(seen), n)
	}
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Server.Close did not return")
	}
	if _, _, err := s.Read(); !errors.Is(err, ErrServerClosed) {
		t.Errorf("Read after Close: err = %v, want ErrServerClosed", err)
	}
	s.Close() // idempotent
}

func TestConnectTimeout(t *testing.T) {
	leakCheck(t)
	// Reserve a port, then free it so nothing answers there.
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := pc.LocalAddr().String()
	pc.Close()

	p := &Params{EpochMs: 10, EpochLimit: 5}
	start := time.Now()
	_, err = NewClient(addr, p)
	if !errors.Is(err, ErrConnectTimeout) {
		t.Fatalf("err = %v, want ErrConnectTimeout", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("gave up after %v, want about %v", d, 5*p.Epoch())
	}
}

// A retried Connect from the same address must get the same connection ID;
// a different address gets a different one.
func TestConnectIsDeduplicatedByAddress(t *testing.T) {
	leakCheck(t)
	s := mustServer(t, patient())
	defer s.Close()
	srvAddr, _ := net.ResolveUDPAddr("udp4", fmt.Sprintf("127.0.0.1:%d", s.Port()))

	connect := func(pc net.PacketConn) uint32 {
		t.Helper()
		pc.WriteTo(marshal(typeConnect, 0, 0, nil), srvAddr)
		buf := make([]byte, maxPacket)
		pc.SetReadDeadline(time.Now().Add(2 * time.Second))
		for {
			n, _, err := pc.ReadFrom(buf)
			if err != nil {
				t.Fatal(err)
			}
			if p, ok := parse(buf[:n]); ok && p.typ == typeConnAck {
				return p.connID
			}
		}
	}
	a, _ := net.ListenPacket("udp4", "127.0.0.1:0")
	defer a.Close()
	b, _ := net.ListenPacket("udp4", "127.0.0.1:0")
	defer b.Close()

	first, again, other := connect(a), connect(a), connect(b)
	if first == 0 {
		t.Error("server assigned the reserved connection ID 0")
	}
	if first != again {
		t.Errorf("retried Connect got ID %d, first got %d", again, first)
	}
	if other == first {
		t.Errorf("two addresses share connection ID %d", first)
	}
}

// Many connections at once: the single read loop must route correctly.
func TestManyConnections(t *testing.T) {
	leakCheck(t)
	const clients, each = 8, 50
	s := mustServer(t, patient())
	defer s.Close()

	var wg sync.WaitGroup
	ids := make([]int, clients)
	for i := range ids {
		c := mustClient(t, s, patient())
		ids[i] = c.ID()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer c.Close()
			for j := 0; j < each; j++ {
				c.Write(msg(j))
			}
			// Expect one reply per distinct message.
			seen := map[int]bool{}
			for len(seen) < each {
				data, err := c.Read()
				if err != nil {
					t.Errorf("conn %d: %v", c.ID(), err)
					return
				}
				seen[idx(data)] = true
			}
		}()
	}

	go func() { // echo server
		for {
			h, data, err := s.Read()
			if errors.Is(err, ErrServerClosed) {
				return
			}
			if err == nil {
				h.Write(data)
			}
		}
	}()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("echo across many connections timed out")
	}
}
