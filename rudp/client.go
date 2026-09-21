package rudp

import (
	"net"
	"time"
)

// Client is one connection to a Server: one socket, one readLoop parked in the
// socket read, one mainLoop owning the connection state.
type Client struct {
	sock net.PacketConn
	conn *conn

	readCh   chan delivery // mainLoop -> Read()
	mainDone chan struct{} // closed when the mainLoop goroutine has returned
	readDone chan struct{} // closed when the readLoop goroutine has returned
}

// NewClient connects to hostport. It sends Connect once per epoch until the
// server answers with a ConnAck carrying the assigned connection ID, and gives
// up with ErrConnectTimeout after EpochLimit epochs. p may be nil.
func NewClient(hostport string, p *Params) (*Client, error) {
	p = p.withDefaults()
	raddr, err := net.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}
	network := "udp4"
	if raddr.IP != nil && raddr.IP.To4() == nil {
		network = "udp6"
	}
	sock, err := net.ListenPacket(network, ":0")
	if err != nil {
		return nil, err
	}
	if p.WrapSocket != nil {
		sock = p.WrapSocket(sock)
	}

	c := &Client{
		sock:     sock,
		readCh:   make(chan delivery),
		mainDone: make(chan struct{}),
		readDone: make(chan struct{}),
	}
	c.conn = newConn(0, true, sock, raddr, p, c.readCh, nil)

	connected := make(chan error, 1)
	go c.readLoop()
	go c.run(connected)
	if err := <-connected; err != nil {
		sock.Close()
		<-c.readDone
		return nil, err
	}
	return c, nil
}

// ID returns the connection ID the server assigned.
func (c *Client) ID() int { return c.conn.handle.ID() }

// Read blocks until a message arrives or the connection ends. Messages may
// arrive in any order and more than once.
func (c *Client) Read() ([]byte, error) {
	select {
	case d := <-c.readCh:
		return d.data, d.err
	case <-c.mainDone:
		return nil, ErrConnClosed
	}
}

// Write queues data for reliable delivery. It never blocks on the send window
// and never fails.
func (c *Client) Write(data []byte) { c.conn.handle.Write(data) }

// Close waits until everything written so far has been acknowledged (or the
// server turns out to be gone), tells the server, and releases the socket.
func (c *Client) Close() error {
	select {
	case c.conn.closeCh <- struct{}{}:
	case <-c.mainDone:
	}
	<-c.mainDone
	c.sock.Close()
	<-c.readDone
	return nil
}

// readLoop parks in the blocking socket read and forwards what it parses.
// It ends when the socket is closed.
func (c *Client) readLoop() {
	defer close(c.readDone)
	buf := make([]byte, maxPacket+1)
	for {
		n, _, err := c.sock.ReadFrom(buf)
		if err != nil {
			return
		}
		p, ok := parse(buf[:n])
		if !ok {
			continue
		}
		select {
		case c.conn.inCh <- p:
		case <-c.conn.dead: // connection has ended: drop, never block
		}
	}
}

// run performs the handshake, then becomes the connection's mainLoop.
func (c *Client) run(connected chan<- error) {
	defer close(c.mainDone)
	cn := c.conn
	ticker := time.NewTicker(cn.epoch)
	connect := func() { cn.sock.WriteTo(marshal(typeConnect, 0, 0, nil), cn.raddr) }

	connect()
	for epochs := 0; ; {
		select {
		case p := <-cn.inCh:
			if p.typ != typeConnAck || p.connID == 0 {
				continue
			}
			ticker.Stop()
			cn.id = p.connID
			cn.handle.id = int(p.connID)
			connected <- nil
			cn.mainLoop()
			return
		case <-ticker.C:
			if epochs++; epochs >= cn.p.EpochLimit {
				ticker.Stop()
				close(cn.dead)
				connected <- ErrConnectTimeout
				return
			}
			connect()
		}
	}
}
