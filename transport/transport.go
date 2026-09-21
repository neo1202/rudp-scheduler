// Package transport is the seam between the scheduler and whatever carries
// its messages. The scheduler needs very little: one Read that reports
// messages and lost connections from every peer, and a per-connection Write
// that never blocks. It needs no ordering and tolerates duplicates, so the
// contract below asks for neither.
//
// Two implementations exist: the repository's own reliable UDP (the point of
// the exercise), and TCP, which is here as the baseline to measure against.
package transport

import (
	"errors"

	"github.com/neo1202/rudp-scheduler/rudp"
)

// ErrClosed is returned by Server.Read, with a nil Conn, once the server has
// shut down.
var ErrClosed = errors.New("transport: server closed")

// Conn is the server's handle on one peer.
type Conn interface {
	// ID is unique among the connections of one server process.
	ID() int
	// Write queues a message. It never blocks and never fails; if the
	// connection is gone the message is dropped.
	Write([]byte)
	// Done is closed once the connection has ended.
	Done() <-chan struct{}
}

// Server accepts connections and funnels what they say into one Read.
type Server interface {
	// Read blocks until any connection has a message, or has ended (non-nil
	// Conn and non-nil error), or the server is closed (nil Conn, ErrClosed).
	// Messages may arrive in any order and more than once.
	Read() (Conn, []byte, error)
	Close() error
}

// Client is one connection to a Server.
type Client interface {
	ID() int
	Read() ([]byte, error)
	Write([]byte)
	// Close delivers what was written before it, then ends the connection.
	Close() error
}

// FromRUDP adapts the reliable-UDP server. *rudp.Client already satisfies
// Client as it is.
func FromRUDP(s *rudp.Server) Server { return rudpServer{s} }

type rudpServer struct{ s *rudp.Server }

func (r rudpServer) Read() (Conn, []byte, error) {
	h, data, err := r.s.Read()
	if h == nil { // keep a nil handle from becoming a non-nil interface
		return nil, nil, ErrClosed
	}
	return h, data, err
}

func (r rudpServer) Close() error { return r.s.Close() }

var _ Client = (*rudp.Client)(nil)
