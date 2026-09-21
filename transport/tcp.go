package transport

import (
	"encoding/binary"
	"io"
	"net"
	"strconv"
	"time"
)

// TCP framing: a 2-byte big-endian length, then the message.
//
// The TCP transport follows the same discipline as the rest of the
// repository: no locks, one owner per piece of state. A connection has a
// reader goroutine (parked in the socket read), a queue goroutine that owns
// the unbounded write backlog so that Write never blocks, and a writer
// goroutine parked in the socket write.

const tcpKeepAlive = time.Second

// tcpConn is one TCP connection, on either side.
type tcpConn struct {
	id       int
	nc       net.Conn
	writeCh  chan []byte   // Write() -> queue
	toWriter chan []byte   // queue -> writer
	closeCh  chan struct{} // Close() -> queue: drain, then hang up
	dead     chan struct{} // closed by the reader when the connection ends
}

func newTCPConn(id int, nc net.Conn) *tcpConn {
	if t, ok := nc.(*net.TCPConn); ok {
		t.SetKeepAlive(true)
		t.SetKeepAlivePeriod(tcpKeepAlive)
	}
	c := &tcpConn{
		id:       id,
		nc:       nc,
		writeCh:  make(chan []byte),
		toWriter: make(chan []byte),
		closeCh:  make(chan struct{}),
		dead:     make(chan struct{}),
	}
	go c.queue()
	go c.writer()
	return c
}

func (c *tcpConn) ID() int               { return c.id }
func (c *tcpConn) Done() <-chan struct{} { return c.dead }

func (c *tcpConn) Write(data []byte) {
	data = append([]byte(nil), data...)
	select {
	case c.writeCh <- data:
	case <-c.dead:
	}
}

// queue owns the backlog. The nil-channel trick disables the hand-off case
// while there is nothing to hand off.
func (c *tcpConn) queue() {
	var pending [][]byte
	closing := false
	for {
		var out chan<- []byte
		var next []byte
		if len(pending) > 0 {
			out, next = c.toWriter, pending[0]
		} else if closing {
			close(c.toWriter) // the writer hangs up once its last write has returned
			return
		}
		select {
		case d := <-c.writeCh:
			if !closing {
				pending = append(pending, d)
			}
		case out <- next:
			pending[0] = nil
			pending = pending[1:]
		case <-c.closeCh:
			closing = true
		case <-c.dead:
			return
		}
	}
}

func (c *tcpConn) writer() {
	var head [2]byte
	for {
		select {
		case d, ok := <-c.toWriter:
			if !ok { // Close: everything written before it has gone out
				c.nc.Close()
				return
			}
			binary.BigEndian.PutUint16(head[:], uint16(len(d)))
			if _, err := c.nc.Write(append(head[:], d...)); err != nil {
				c.nc.Close() // the reader notices and declares the connection dead
				return
			}
		case <-c.dead:
			return
		}
	}
}

// read parks in the socket and forwards frames to out until the connection
// ends, then closes dead (it is the only goroutine that does) and reports the
// error. stop is closed when nobody will read out any more.
func (c *tcpConn) read(out chan<- tcpDelivery, stop <-chan struct{}) {
	var head [2]byte
	var err error
	for {
		if _, err = io.ReadFull(c.nc, head[:]); err != nil {
			break
		}
		data := make([]byte, binary.BigEndian.Uint16(head[:]))
		if _, err = io.ReadFull(c.nc, data); err != nil {
			break
		}
		select {
		case out <- tcpDelivery{c, data, nil}:
		case <-stop:
		}
	}
	c.nc.Close()
	close(c.dead)
	select {
	case out <- tcpDelivery{c, nil, err}:
	case <-stop:
	}
}

func (c *tcpConn) close() {
	select {
	case c.closeCh <- struct{}{}:
	case <-c.dead:
	}
}

type tcpDelivery struct {
	c    *tcpConn
	data []byte
	err  error
}

// TCPServer is the TCP implementation of Server.
type TCPServer struct {
	ln      net.Listener
	readCh  chan tcpDelivery
	closing chan struct{} // closed by the accept loop when it ends
	done    chan struct{}
}

// ListenTCP starts a TCP server on the given port (0 picks a free one).
func ListenTCP(port int) (*TCPServer, error) {
	ln, err := net.Listen("tcp", net.JoinHostPort("", strconv.Itoa(port)))
	if err != nil {
		return nil, err
	}
	s := &TCPServer{ln: ln, readCh: make(chan tcpDelivery), closing: make(chan struct{}), done: make(chan struct{})}
	go s.accept()
	return s, nil
}

// Port returns the TCP port the server is bound to.
func (s *TCPServer) Port() int { return s.ln.Addr().(*net.TCPAddr).Port }

// accept owns the connection list. When the listener is closed it hangs up
// on everyone.
func (s *TCPServer) accept() {
	defer close(s.done)
	var conns []*tcpConn
	for id := 1; ; id++ {
		nc, err := s.ln.Accept()
		if err != nil {
			close(s.closing)
			for _, c := range conns {
				c.nc.Close()
			}
			return
		}
		live := conns[:0]
		for _, c := range conns { // forget connections that have ended
			select {
			case <-c.dead:
			default:
				live = append(live, c)
			}
		}
		c := newTCPConn(id, nc)
		conns = append(live, c)
		go c.read(s.readCh, s.closing)
	}
}

func (s *TCPServer) Read() (Conn, []byte, error) {
	select {
	case d := <-s.readCh:
		return d.c, d.data, d.err
	case <-s.done:
		return nil, nil, ErrClosed
	}
}

// Close stops accepting and ends every connection. It may be called more
// than once.
func (s *TCPServer) Close() error {
	s.ln.Close()
	<-s.done
	return nil
}

// TCPClient is the TCP implementation of Client.
type TCPClient struct {
	c          *tcpConn
	readCh     chan tcpDelivery
	readerDone chan struct{} // closed when the reader goroutine has returned
	stop       chan struct{} // closed by the first Close: nobody will Read any more
	closeToken chan struct{} // holds one token; whoever takes it closes stop
}

// DialTCP connects to a TCPServer.
func DialTCP(hostport string) (*TCPClient, error) {
	nc, err := net.DialTimeout("tcp", hostport, 2*time.Second)
	if err != nil {
		return nil, err
	}
	cl := &TCPClient{
		c:          newTCPConn(0, nc),
		readCh:     make(chan tcpDelivery),
		readerDone: make(chan struct{}),
		stop:       make(chan struct{}),
		closeToken: make(chan struct{}, 1),
	}
	cl.closeToken <- struct{}{}
	go func() {
		cl.c.read(cl.readCh, cl.stop)
		close(cl.readerDone)
	}()
	return cl, nil
}

func (cl *TCPClient) ID() int           { return cl.c.id }
func (cl *TCPClient) Write(data []byte) { cl.c.Write(data) }

func (cl *TCPClient) Read() ([]byte, error) {
	select {
	case d := <-cl.readCh:
		return d.data, d.err
	case <-cl.readerDone:
		return nil, io.EOF
	}
}

// Close sends what was written before it, then hangs up. It may be called
// more than once.
func (cl *TCPClient) Close() error {
	cl.c.close()
	select {
	case <-cl.closeToken:
		close(cl.stop)
	default:
	}
	<-cl.readerDone
	return nil
}
