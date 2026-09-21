package rudp

import (
	"errors"
	"fmt"
	"net"
	"os"
	"time"
)

// Server accepts any number of connections on a single UDP port.
//
// A UDP socket has no notion of connections, so one goroutine (readLoop) reads
// every datagram and routes it by connection ID to that connection's mainLoop.
// Read is the single exit for data from all connections.
type Server struct {
	sock net.PacketConn
	p    *Params

	readCh   chan delivery // every connection's mainLoop -> Read()
	closeReq chan struct{} // Close() -> readLoop, capacity 1
	closing  chan struct{} // closed by readLoop when shutdown begins
	done     chan struct{} // closed by readLoop when it has exited
}

// NewServer listens on the given UDP port (0 picks a free one). p may be nil.
func NewServer(port int, p *Params) (*Server, error) {
	p = p.withDefaults()
	sock, err := net.ListenPacket("udp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}
	if p.WrapSocket != nil {
		sock = p.WrapSocket(sock)
	}
	s := &Server{
		sock:     sock,
		p:        p,
		readCh:   make(chan delivery),
		closeReq: make(chan struct{}, 1),
		closing:  make(chan struct{}),
		done:     make(chan struct{}),
	}
	go s.readLoop()
	return s, nil
}

// Port returns the UDP port the server is bound to.
func (s *Server) Port() int {
	if a, ok := s.sock.LocalAddr().(*net.UDPAddr); ok {
		return a.Port
	}
	return 0
}

// Read blocks until any connection has something to report. A non-nil error
// with a non-nil handle means that connection is gone; ErrServerClosed (with a
// nil handle) means the server itself has shut down.
func (s *Server) Read() (*ConnHandle, []byte, error) {
	select {
	case d := <-s.readCh:
		return d.h, d.data, d.err
	case <-s.done:
		return nil, nil, ErrServerClosed
	}
}

// Close stops accepting connections, lets every open connection flush what it
// still owes its peer, then releases the socket. It may be called more than
// once.
func (s *Server) Close() error {
	select {
	case s.closeReq <- struct{}{}:
	default:
	}
	<-s.done
	return nil
}

// readLoop parks in the blocking socket read so nobody else has to, and turns
// datagrams into channel messages. It alone owns the connection table.
func (s *Server) readLoop() {
	defer close(s.done)
	var (
		conns     = map[uint32]*conn{} // only readLoop touches these
		byAddr    = map[string]uint32{}
		nextID    uint32
		closing   bool
		epoch     = s.p.Epoch()
		lastSweep = time.Now()
		buf       = make([]byte, maxPacket+1)
	)
	forget := func(id uint32, c *conn) {
		delete(conns, id)
		if key := c.raddr.String(); byAddr[key] == id {
			delete(byAddr, key)
		}
	}
	beginShutdown := func() {
		closing = true
		close(s.closing)
		for _, c := range conns {
			c.handle.Close()
		}
	}
	defer func() {
		if !closing {
			close(s.closing)
		}
	}()

	for {
		if !closing {
			select {
			case <-s.closeReq:
				beginShutdown()
			default:
			}
		}
		if now := time.Now(); now.Sub(lastSweep) >= epoch {
			lastSweep = now
			for id, c := range conns { // forget connections that have ended
				if c.isDead() {
					forget(id, c)
				}
			}
		}
		if closing && len(conns) == 0 {
			s.sock.Close()
			return
		}

		s.sock.SetReadDeadline(time.Now().Add(epoch))
		n, addr, err := s.sock.ReadFrom(buf) // blocks: a datagram, or the deadline
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				continue
			}
			if errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		p, ok := parse(buf[:n])
		if !ok {
			continue
		}

		if p.typ == typeConnect {
			if closing {
				continue
			}
			// De-duplicate by remote address: a retried Connect gets the
			// same connection ID back.
			key := addr.String()
			id, known := byAddr[key]
			if old := conns[id]; known && old.isDead() {
				forget(id, old)
				known = false
			}
			if !known {
				nextID++
				id = nextID
				c := newConn(id, false, s.sock, addr, s.p, s.readCh, s.closing)
				conns[id], byAddr[key] = c, id
				go c.mainLoop()
			}
			s.sock.WriteTo(marshal(typeConnAck, id, 0, nil), addr)
			continue
		}

		if c, ok := conns[p.connID]; ok {
			select {
			case c.inCh <- p: // hand over to that connection's mainLoop
			case <-c.dead: // it has ended: drop, never block
				forget(p.connID, c)
			}
		}
	}
}
