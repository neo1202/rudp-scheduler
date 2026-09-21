// Package rudp is a small reliable-datagram transport on top of UDP.
//
// Its contract is deliberately weaker than TCP's:
//
//   - every message handed to Write is delivered to the peer's Read at least
//     once, as long as the connection stays up;
//   - messages may be delivered in any order;
//   - a message may be delivered more than once.
//
// There is no reorder buffer and no receive-side de-duplication on the default
// path. A message is acknowledged and handed to the application the moment it
// arrives, so one lost packet never delays the packets behind it (no
// head-of-line blocking). Applications are expected to be idempotent.
//
// The package uses no locks. Every connection's state is owned by exactly one
// goroutine (its mainLoop); everything else talks to it over channels.
package rudp

import (
	"errors"
	"flag"
	"net"
	"time"
)

// Errors reported by Read.
var (
	// ErrConnLost means nothing was heard from the peer for EpochLimit epochs.
	ErrConnLost = errors.New("rudp: connection lost")
	// ErrConnClosed means the peer closed the connection, or Read was called
	// on a connection that has already ended.
	ErrConnClosed = errors.New("rudp: connection closed")
	// ErrServerClosed is returned by Server.Read after Close.
	ErrServerClosed = errors.New("rudp: server closed")
	// ErrConnectTimeout is returned by NewClient when no ConnAck arrives
	// within EpochLimit epochs.
	ErrConnectTimeout = errors.New("rudp: connect timed out")
)

// Params holds every transport tunable. The zero value of any field means
// "use the default".
type Params struct {
	EpochMs    int // length of one epoch in milliseconds (default 20)
	EpochLimit int // silent epochs before the connection is declared lost (default 5)
	Window     int // maximum number of unacknowledged messages in flight (default 5)
	MaxBackoff int // upper bound of a message's RTO, in epochs (default 8)

	// OrderedDelivery switches the receive path to strict in-order,
	// exactly-once delivery. It exists only so benchmarks can measure what
	// head-of-line blocking costs; see ordered.go. Leave it false.
	OrderedDelivery bool

	// WrapSocket, if set, is applied to the UDP socket right after it is
	// opened. Tests and benchmarks use it to inject a lossy network.
	WrapSocket func(net.PacketConn) net.PacketConn

	// Stats, if set, receives cumulative per-connection snapshots. Sends are
	// non-blocking: a full channel drops a snapshot, never stalls a connection.
	Stats chan<- ConnStats

	// Hooks are test probes, called on the goroutine that owns the state.
	Hooks *Hooks
}

// Hooks lets tests observe internal events without sharing state.
type Hooks struct {
	// OnBacklog fires when a Write could not enter the window and had to
	// wait in pendingWrites. depth is the queue length after the append.
	OnBacklog func(connID int, depth int)
}

// Default values, from the design's parameter table.
const (
	DefaultEpochMs    = 20
	DefaultEpochLimit = 5
	DefaultWindow     = 5
	DefaultMaxBackoff = 8
)

// DefaultParams returns the default tunables.
func DefaultParams() *Params {
	return (&Params{}).withDefaults()
}

// withDefaults returns a copy of p with every zero field filled in. A nil
// receiver yields the defaults.
func (p *Params) withDefaults() *Params {
	var q Params
	if p != nil {
		q = *p
	}
	if q.EpochMs <= 0 {
		q.EpochMs = DefaultEpochMs
	}
	if q.EpochLimit <= 0 {
		q.EpochLimit = DefaultEpochLimit
	}
	if q.Window <= 0 {
		q.Window = DefaultWindow
	}
	if q.MaxBackoff <= 0 {
		q.MaxBackoff = DefaultMaxBackoff
	}
	return &q
}

// Epoch returns the epoch length as a duration.
func (p *Params) Epoch() time.Duration {
	return time.Duration(p.withDefaults().EpochMs) * time.Millisecond
}

// RegisterFlags binds the transport tunables to fs. OrderedDelivery is
// intentionally not exposed; only the benchmark harness sets it.
func (p *Params) RegisterFlags(fs *flag.FlagSet) {
	d := p.withDefaults()
	fs.IntVar(&p.EpochMs, "epoch-ms", d.EpochMs, "transport epoch length in milliseconds")
	fs.IntVar(&p.EpochLimit, "epoch-limit", d.EpochLimit, "silent epochs before a connection is declared lost")
	fs.IntVar(&p.Window, "window", d.Window, "max unacknowledged messages per connection (also the per-worker in-flight cap)")
	fs.IntVar(&p.MaxBackoff, "max-backoff", d.MaxBackoff, "upper bound of a message's retransmission timeout, in epochs")
}

// ConnStats is a cumulative snapshot of one connection's counters, published
// by the connection's mainLoop.
type ConnStats struct {
	ConnID        int
	Client        bool // true when published by a client endpoint
	DataSent      uint64
	Retransmits   uint64
	DataReceived  uint64 // includes duplicates
	AcksSent      uint64
	Heartbeats    uint64
	SRTT          time.Duration
	SendBuffer    int
	PendingWrites int
	Closed        bool // final snapshot: the connection has ended
}
