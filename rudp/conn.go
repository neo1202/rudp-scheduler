package rudp

import (
	"net"
	"time"
)

// delivery is what a connection hands to the application: a payload, or the
// error that ended the connection.
type delivery struct {
	h    *ConnHandle
	data []byte
	err  error
}

// outMsg is one sent-but-unacknowledged message.
type outMsg struct {
	payload       []byte
	firstSent     time.Time
	lastSent      time.Time
	rto           time.Duration
	retries       int
	retransmitted bool
}

// ConnHandle is how the application addresses one connection. It carries the
// connection's channels, so writing never needs a lookup in the connection
// table; that table can therefore belong to the read loop alone.
type ConnHandle struct {
	id         int
	writeReqCh chan<- []byte
	closeCh    chan<- struct{}
	dead       <-chan struct{}
}

// ID returns the connection ID the server assigned.
func (h *ConnHandle) ID() int { return h.id }

// Write queues data for reliable delivery. It never blocks on the send window
// and never fails: if the connection is already gone the data is dropped.
// Payloads larger than MaxPayload are a programming error and panic.
func (h *ConnHandle) Write(data []byte) {
	if len(data) > MaxPayload {
		panic("rudp: payload exceeds MaxPayload")
	}
	data = append([]byte(nil), data...)
	select {
	case h.writeReqCh <- data: // mainLoop is always in its select; taken almost at once
	case <-h.dead: // connection has ended: drop
	}
}

// Close asks the connection to flush everything it still owes the peer and
// then end. It returns immediately.
func (h *ConnHandle) Close() {
	select {
	case h.closeCh <- struct{}{}:
	case <-h.dead:
	}
}

// Done is closed once the connection has ended for any reason.
func (h *ConnHandle) Done() <-chan struct{} { return h.dead }

// conn is one connection. Every field below "owned by mainLoop" is read and
// written by that goroutine only.
type conn struct {
	id     uint32
	client bool
	sock   net.PacketConn
	raddr  net.Addr
	p      *Params
	epoch  time.Duration
	handle *ConnHandle

	inCh       chan packet     // read loop -> mainLoop
	writeReqCh chan []byte     // Write() -> mainLoop
	closeCh    chan struct{}   // Close() -> mainLoop
	dead       chan struct{}   // closed by mainLoop when the connection ends
	out        chan<- delivery // mainLoop -> Read()
	stop       <-chan struct{} // closed when nobody will call Read any more

	// owned by mainLoop
	sendBuffer    map[uint32]*outMsg // sent, awaiting ACK; len <= Window
	pendingWrites [][]byte           // FIFO of writes that do not fit the window yet
	deliverQ      []delivery         // arrival-order hand-off to Read()
	nextSeq       uint32
	srtt          time.Duration
	hasSRTT       bool
	epochsSilent  int
	sentThisEpoch bool
	accept        func(seq uint32, payload []byte) // receive path; see ordered.go
	stats         ConnStats
	statsDirty    bool
	lastPublish   time.Time
}

func newConn(id uint32, client bool, sock net.PacketConn, raddr net.Addr, p *Params,
	out chan<- delivery, stop <-chan struct{}) *conn {
	c := &conn{
		id:         id,
		client:     client,
		sock:       sock,
		raddr:      raddr,
		p:          p,
		epoch:      p.Epoch(),
		inCh:       make(chan packet, 64),
		writeReqCh: make(chan []byte),
		closeCh:    make(chan struct{}),
		dead:       make(chan struct{}),
		out:        out,
		stop:       stop,
		sendBuffer: make(map[uint32]*outMsg),
	}
	c.handle = &ConnHandle{id: int(id), writeReqCh: c.writeReqCh, closeCh: c.closeCh, dead: c.dead}
	c.accept = c.deliverOnArrival
	if p.OrderedDelivery {
		installOrderedDelivery(c)
	}
	return c
}

func (c *conn) isDead() bool {
	select {
	case <-c.dead:
		return true
	default:
		return false
	}
}

// mainLoop is the only goroutine that touches this connection's state. It
// waits on five things at once; whichever happens first is handled, then it
// goes back to waiting. None of the handlers block.
func (c *conn) mainLoop() {
	ticker := time.NewTicker(c.epoch)
	defer ticker.Stop()
	for {
		var out chan<- delivery // nil disables the hand-off case
		var next delivery
		if len(c.deliverQ) > 0 {
			out, next = c.out, c.deliverQ[0]
		}

		select {
		case p := <-c.inCh: // a packet routed here by the read loop
			if p.connID != c.id {
				continue
			}
			c.epochsSilent = 0
			switch p.typ {
			case typeAck:
				c.onAck(p.seq)
			case typeData:
				if p.seq == 0 {
					continue
				}
				c.stats.DataReceived++
				c.statsDirty = true
				c.accept(p.seq, p.payload)
				c.sendAck(p.seq) // duplicates are acknowledged too
			case typeClose:
				c.drainAndExit(ErrConnClosed)
				return
			}

		case data := <-c.writeReqCh: // the application called Write
			c.pendingWrites = append(c.pendingWrites, data)
			c.fillWindow()
			if n := len(c.pendingWrites); n > 0 && c.p.Hooks != nil && c.p.Hooks.OnBacklog != nil {
				c.p.Hooks.OnBacklog(int(c.id), n)
			}

		case out <- next: // the application's Read took one
			c.deliverQ[0] = delivery{}
			c.deliverQ = c.deliverQ[1:]

		case <-ticker.C: // one epoch passed
			c.retransmitExpired()
			c.heartbeat()
			c.epochsSilent++
			if c.epochsSilent >= c.p.EpochLimit {
				c.drainAndExit(ErrConnLost)
				return
			}
			c.publishStats(false)

		case <-c.closeCh: // the application called Close
			c.flushThenExit(ticker)
			c.publishStats(true)
			close(c.dead)
			return
		}
	}
}

// deliverOnArrival is the default receive path: hand the payload up right
// away, in arrival order. Nothing is reordered and nothing is de-duplicated;
// ordering and duplicates are the application's business.
func (c *conn) deliverOnArrival(_ uint32, payload []byte) {
	c.deliverQ = append(c.deliverQ, delivery{h: c.handle, data: payload})
}

func (c *conn) onAck(seq uint32) {
	m, ok := c.sendBuffer[seq]
	if !ok {
		return // heartbeat (seq 0), or an ACK for something already acknowledged
	}
	if !m.retransmitted {
		// Karn: an ACK for a retransmitted message is ambiguous, skip it.
		c.sampleRTT(time.Since(m.firstSent))
	}
	delete(c.sendBuffer, seq)
	c.fillWindow()
}

func (c *conn) sampleRTT(sample time.Duration) {
	if !c.hasSRTT {
		c.srtt, c.hasSRTT = sample, true
	} else {
		c.srtt = (7*c.srtt + sample) / 8 // 0.875*srtt + 0.125*sample
	}
	c.statsDirty = true
}

// fillWindow moves writes from the head of pendingWrites into the window.
func (c *conn) fillWindow() {
	for len(c.pendingWrites) > 0 && len(c.sendBuffer) < c.p.Window {
		payload := c.pendingWrites[0]
		c.pendingWrites[0] = nil
		c.pendingWrites = c.pendingWrites[1:]

		c.nextSeq++
		now := time.Now()
		c.sendBuffer[c.nextSeq] = &outMsg{payload: payload, firstSent: now, lastSent: now, rto: c.initialRTO()}
		c.send(typeData, c.nextSeq, payload)
		c.stats.DataSent++
		c.statsDirty = true
	}
}

// initialRTO is max(2*srtt, 1 epoch), or 2 epochs before the first sample.
func (c *conn) initialRTO() time.Duration {
	if !c.hasSRTT {
		return 2 * c.epoch
	}
	if rto := 2 * c.srtt; rto > c.epoch {
		return rto
	}
	return c.epoch
}

// retransmitExpired resends every message whose RTO has run out and doubles
// that message's RTO, capped at MaxBackoff epochs. The scan is bounded by the
// window size.
func (c *conn) retransmitExpired() {
	now := time.Now()
	limit := time.Duration(c.p.MaxBackoff) * c.epoch
	for seq, m := range c.sendBuffer {
		if now.Sub(m.lastSent) < m.rto {
			continue
		}
		m.lastSent = now
		m.retries++
		m.retransmitted = true
		if m.rto *= 2; m.rto > limit {
			m.rto = limit
		}
		c.send(typeData, seq, m.payload)
		c.stats.Retransmits++
		c.statsDirty = true
	}
}

// heartbeat keeps an idle connection alive: if nothing at all was sent during
// the epoch that just ended, send an Ack with the reserved seq 0.
func (c *conn) heartbeat() {
	if !c.sentThisEpoch {
		c.send(typeAck, 0, nil)
		c.stats.Heartbeats++
	}
	c.sentThisEpoch = false
}

func (c *conn) sendAck(seq uint32) {
	c.send(typeAck, seq, nil)
	c.stats.AcksSent++
}

func (c *conn) send(typ uint8, seq uint32, payload []byte) {
	c.sock.WriteTo(marshal(typ, c.id, seq, payload), c.raddr) // UDP: errors are just loss
	c.sentThisEpoch = true
}

// drainAndExit ends the connection because the peer is gone. dead is closed
// first, so from this point nobody can be blocked on this goroutine; only
// then does it block to hand the backlog and the final error to Read.
func (c *conn) drainAndExit(err error) {
	close(c.dead)
	c.publishStats(true)
	c.deliverQ = append(c.deliverQ, delivery{h: c.handle, err: err})
	for _, d := range c.deliverQ {
		select {
		case c.out <- d:
		case <-c.stop: // endpoint is shutting down, nobody will Read
			return
		case <-c.closeCh: // client endpoint: Close called while draining
			return
		}
	}
}

// flushThenExit keeps the send side running until everything written before
// Close has been acknowledged, or the peer turns out to be gone. Incoming data
// is still acknowledged but no longer delivered; the application has left.
func (c *conn) flushThenExit(ticker *time.Ticker) {
	c.fillWindow()
	for len(c.sendBuffer) > 0 || len(c.pendingWrites) > 0 {
		select {
		case p := <-c.inCh:
			if p.connID != c.id {
				continue
			}
			c.epochsSilent = 0
			switch p.typ {
			case typeAck:
				c.onAck(p.seq)
			case typeData:
				c.sendAck(p.seq)
			case typeClose:
				return
			}
		case <-c.writeReqCh: // written after Close: discarded
		case <-ticker.C:
			c.retransmitExpired()
			c.heartbeat()
			c.epochsSilent++
			if c.epochsSilent >= c.p.EpochLimit {
				return
			}
		}
	}
	c.send(typeClose, 0, nil) // best effort; the peer falls back to the silence timeout
}

// publishStats sends a cumulative snapshot. It never blocks: counters are
// cumulative, so a dropped snapshot is repaired by the next one.
func (c *conn) publishStats(final bool) {
	if c.p.Stats == nil {
		return
	}
	now := time.Now()
	if !final && !c.statsDirty && now.Sub(c.lastPublish) < time.Second {
		return
	}
	s := c.stats
	s.ConnID = int(c.id)
	s.Client = c.client
	s.SRTT = c.srtt
	s.SendBuffer = len(c.sendBuffer)
	s.PendingWrites = len(c.pendingWrites)
	s.Closed = final
	select {
	case c.p.Stats <- s:
		c.statsDirty = false
		c.lastPublish = now
	default:
	}
}
