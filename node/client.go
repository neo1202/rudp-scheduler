package node

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"

	"github.com/neo1202/rudp-scheduler/rudp"
	"github.com/neo1202/rudp-scheduler/wire"
	"github.com/neo1202/rudp-scheduler/workload"
)

// NewJobID returns a random job ID. The client owns the ID, so the job keeps
// its identity across reconnects and server restarts.
func NewJobID() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // the OS random source is gone; nothing sensible to do
	}
	return binary.BigEndian.Uint64(b[:])
}

// Submit sends one request over c and blocks until the Result for that job
// arrives or the connection ends. A connection carries one request.
//
// Submitting the same job ID again, on a new connection, is safe and is how a
// client recovers from a lost connection or a restarted server: it joins the
// job if it is still running, or gets the stored answer if it has finished.
func Submit(c *rudp.Client, job uint64, msg string, lo, hi uint64) (workload.Partial, error) {
	if len(msg) > wire.MaxMsgLen {
		return workload.Partial{}, fmt.Errorf("node: message is %d bytes, limit is %d", len(msg), wire.MaxMsgLen)
	}
	c.Write(wire.Encode(wire.Request{Job: job, Lo: lo, Hi: hi, Msg: msg}))
	for {
		data, err := c.Read()
		if err != nil {
			return workload.Partial{}, err
		}
		if m, err := wire.Decode(data); err == nil {
			if r, ok := m.(wire.Result); ok && r.Job == job {
				return workload.Partial{Hash: r.Hash, Nonce: r.Nonce}, nil
			}
		}
	}
}
