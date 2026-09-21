package node

import (
	"fmt"

	"github.com/neo1202/rudp-scheduler/rudp"
	"github.com/neo1202/rudp-scheduler/wire"
	"github.com/neo1202/rudp-scheduler/workload"
)

// Submit sends one request over c and blocks until its Result arrives or the
// connection ends. A connection carries one request.
func Submit(c *rudp.Client, msg string, lo, hi uint64) (workload.Partial, error) {
	if len(msg) > wire.MaxMsgLen {
		return workload.Partial{}, fmt.Errorf("node: message is %d bytes, limit is %d", len(msg), wire.MaxMsgLen)
	}
	c.Write(wire.Encode(wire.Request{Lo: lo, Hi: hi, Msg: msg}))
	for {
		data, err := c.Read()
		if err != nil {
			return workload.Partial{}, err
		}
		if m, err := wire.Decode(data); err == nil {
			if r, ok := m.(wire.Result); ok {
				return workload.Partial{Hash: r.Hash, Nonce: r.Nonce}, nil
			}
		}
	}
}
