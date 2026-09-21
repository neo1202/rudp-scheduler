package transport

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"testing"
	"time"
)

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

func pair(t *testing.T) (*TCPServer, *TCPClient) {
	t.Helper()
	s, err := ListenTCP(0)
	if err != nil {
		t.Fatal(err)
	}
	c, err := DialTCP(fmt.Sprintf("127.0.0.1:%d", s.Port()))
	if err != nil {
		t.Fatal(err)
	}
	return s, c
}

func num(i int) []byte { return binary.BigEndian.AppendUint32(nil, uint32(i)) }

// Write must not block however far the peer falls behind, and Close must
// deliver everything written before it.
func TestTCPWriteNeverBlocksAndCloseFlushes(t *testing.T) {
	leakCheck(t)
	const n = 20000
	s, c := pair(t)
	defer s.Close()

	start := time.Now()
	for i := 0; i < n; i++ { // nobody is reading on the server yet
		c.Write(append(num(i), make([]byte, 1000)...))
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("%d Writes took %v with an idle reader: Write is blocking", n, d)
	}
	closed := make(chan struct{})
	go func() { c.Close(); close(closed) }()

	var conn Conn
	for i := 0; i < n; i++ {
		h, data, err := s.Read()
		if err != nil {
			t.Fatalf("message %d: %v", i, err)
		}
		if got := int(binary.BigEndian.Uint32(data)); got != i {
			t.Fatalf("message %d arrived as %d", i, got)
		}
		conn = h
	}
	if h, _, err := s.Read(); err == nil || h == nil || h.ID() != conn.ID() {
		t.Fatalf("after the last message Read = (%v, %v), want the connection's end", h, err)
	}
	select {
	case <-conn.Done():
	default:
		t.Error("Done() not closed after the connection ended")
	}
	<-closed
	c.Close() // idempotent
	if _, err := c.Read(); err == nil {
		t.Error("Read on a closed client succeeded")
	}
}

func TestTCPServerCloseEndsEverything(t *testing.T) {
	leakCheck(t)
	s, c := pair(t)
	defer c.Close()
	c.Write([]byte("hi"))
	h, data, err := s.Read()
	if err != nil || string(data) != "hi" {
		t.Fatalf("Read = %q, %v", data, err)
	}
	h.Write([]byte("hello"))
	if data, err := c.Read(); err != nil || string(data) != "hello" {
		t.Fatalf("client Read = %q, %v", data, err)
	}

	s.Close()
	s.Close()
	if h, _, err := s.Read(); h != nil || err != ErrClosed {
		t.Errorf("Read after Close = (%v, %v), want (nil, ErrClosed)", h, err)
	}
	if _, err := c.Read(); err == nil {
		t.Error("client did not notice the server going away")
	}
	h.Write([]byte("into the void")) // must not block or panic
}
