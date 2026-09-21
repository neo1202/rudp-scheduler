// Package lossy wraps a net.PacketConn and misbehaves on purpose: it drops,
// duplicates and delays outgoing packets according to a seeded random source.
//
// It exists so that tests and benchmarks can exercise the transport against a
// hostile network on a single machine, reproducibly. Impairment is applied on
// the write path only; wrap both endpoints to impair both directions.
//
// Like the rest of this repository the wrapper uses no locks. One goroutine
// owns the random source, the delay heap and the counters; WriteTo hands
// packets to it over a channel.
package lossy

import (
	"container/heap"
	"math/rand"
	"net"
	"time"
)

// Config describes how a Conn mistreats outgoing packets.
type Config struct {
	Drop   float64       // probability in [0,1] that a packet is silently dropped
	Dup    float64       // probability in [0,1] that a surviving packet is sent twice
	Jitter time.Duration // each copy is delayed by a uniform duration in [0, Jitter)
	Seed   int64         // seed for the decision sequence

	// Filter, if set, sees every outgoing packet before the random drop and
	// returns true to discard it. It runs on the Conn's own goroutine, so it
	// may keep state without synchronisation.
	Filter func(pkt []byte) bool
}

// Stats counts what happened to the packets handed to WriteTo.
type Stats struct {
	Written    uint64 // packets handed to WriteTo
	Dropped    uint64 // discarded by Filter or by the random drop
	Duplicated uint64 // extra copies sent
	Delayed    uint64 // copies that went through the delay heap
}

// Conn is a net.PacketConn whose writes are unreliable.
type Conn struct {
	net.PacketConn // reads, deadlines and LocalAddr pass straight through

	cfg     Config
	writeCh chan outPacket
	statsCh chan chan Stats
	closeCh chan struct{}
	done    chan struct{}
}

type outPacket struct {
	b    []byte
	addr net.Addr
	due  time.Time
}

// Wrap returns inner with cfg applied to its write path and starts the
// goroutine that owns the impairment state.
func Wrap(inner net.PacketConn, cfg Config) *Conn {
	c := &Conn{
		PacketConn: inner,
		cfg:        cfg,
		writeCh:    make(chan outPacket, 1024),
		statsCh:    make(chan chan Stats),
		closeCh:    make(chan struct{}),
		done:       make(chan struct{}),
	}
	go c.run()
	return c
}

// WriteTo queues b for (possibly unreliable) transmission. It reports success
// even if the packet is later dropped, exactly like a real network would.
func (c *Conn) WriteTo(b []byte, addr net.Addr) (int, error) {
	select {
	case <-c.done: // writeCh is buffered, so check this first
		return 0, net.ErrClosed
	default:
	}
	p := outPacket{b: append([]byte(nil), b...), addr: addr}
	select {
	case c.writeCh <- p:
		return len(b), nil
	case <-c.done:
		return 0, net.ErrClosed
	}
}

// Stats returns a snapshot of the counters.
func (c *Conn) Stats() Stats {
	reply := make(chan Stats, 1)
	select {
	case c.statsCh <- reply:
		return <-reply
	case <-c.done:
		return Stats{}
	}
}

// Close sends whatever is still queued or delayed, then closes the inner
// connection. It is safe to call more than once.
func (c *Conn) Close() error {
	select {
	case c.closeCh <- struct{}{}:
	case <-c.done:
	}
	<-c.done
	return nil
}

func (c *Conn) run() {
	defer close(c.done)
	rng := rand.New(rand.NewSource(c.cfg.Seed))
	var (
		stats   Stats
		delayed delayHeap
		timer   = time.NewTimer(time.Hour)
	)
	timer.Stop()
	defer timer.Stop()

	// admit applies the impairment decisions to one packet. Packets that
	// survive are either written now or parked on the delay heap.
	admit := func(p outPacket, allowDelay bool) {
		stats.Written++
		if c.cfg.Filter != nil && c.cfg.Filter(p.b) {
			stats.Dropped++
			return
		}
		if rng.Float64() < c.cfg.Drop {
			stats.Dropped++
			return
		}
		copies := 1
		if rng.Float64() < c.cfg.Dup {
			copies = 2
			stats.Duplicated++
		}
		for i := 0; i < copies; i++ {
			var d time.Duration
			if c.cfg.Jitter > 0 {
				d = time.Duration(rng.Int63n(int64(c.cfg.Jitter)))
			}
			if d == 0 || !allowDelay {
				c.PacketConn.WriteTo(p.b, p.addr)
				continue
			}
			stats.Delayed++
			p.due = time.Now().Add(d)
			heap.Push(&delayed, p)
		}
	}

	for {
		select {
		case p := <-c.writeCh:
			admit(p, true)
		case <-timer.C:
		case reply := <-c.statsCh:
			reply <- stats
		case <-c.closeCh:
			for {
				select {
				case p := <-c.writeCh:
					admit(p, false)
					continue
				default:
				}
				break
			}
			for delayed.Len() > 0 {
				p := heap.Pop(&delayed).(outPacket)
				c.PacketConn.WriteTo(p.b, p.addr)
			}
			c.PacketConn.Close()
			return
		}

		now := time.Now()
		for delayed.Len() > 0 && !delayed[0].due.After(now) {
			p := heap.Pop(&delayed).(outPacket)
			c.PacketConn.WriteTo(p.b, p.addr)
		}
		if delayed.Len() > 0 {
			timer.Reset(time.Until(delayed[0].due))
		}
	}
}

// delayHeap orders parked packets by due time.
type delayHeap []outPacket

func (h delayHeap) Len() int           { return len(h) }
func (h delayHeap) Less(i, j int) bool { return h[i].due.Before(h[j].due) }
func (h delayHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *delayHeap) Push(x any)        { *h = append(*h, x.(outPacket)) }
func (h *delayHeap) Pop() any {
	old := *h
	n := len(old)
	p := old[n-1]
	old[n-1] = outPacket{}
	*h = old[:n-1]
	return p
}
