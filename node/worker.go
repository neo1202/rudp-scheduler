// Package node holds the run loops of the two kinds of process that talk to a
// server: workers, which compute chunks, and clients, which submit a job and
// wait for its answer. Both are thin; the interesting decisions are all made
// on the server.
package node

import (
	"github.com/neo1202/rudp-scheduler/transport"
	"github.com/neo1202/rudp-scheduler/wire"
	"github.com/neo1202/rudp-scheduler/workload"
)

// RunWorker announces c as a worker and then serves chunks until the
// connection ends, returning the error that ended it.
//
// The worker keeps no memory of what it has done. If the transport delivers
// the same Chunk twice it is computed and answered twice; the server
// de-duplicates by TaskID.
func RunWorker(c transport.Client, wl workload.Workload) error {
	c.Write(wire.Encode(wire.Join{}))
	for {
		data, err := c.Read()
		if err != nil {
			return err
		}
		m, err := wire.Decode(data)
		if err != nil {
			continue
		}
		chunk, ok := m.(wire.Chunk)
		if !ok {
			continue
		}
		p := wl.Compute(chunk.Msg, chunk.Lo, chunk.Hi)
		c.Write(wire.Encode(wire.ChunkResult{ID: chunk.ID, Hash: p.Hash, Nonce: p.Nonce}))
	}
}
