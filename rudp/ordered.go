package rudp

// This file is the whole implementation of Params.OrderedDelivery, a
// benchmark-only switch. It swaps the connection's receive path for one that
// holds messages back until every earlier sequence number has been delivered,
// which is how a TCP-like transport behaves. cmd/bench uses it to measure what
// head-of-line blocking costs this workload.
//
// Nothing outside this file knows about sequence order. The default receive
// path (conn.deliverOnArrival) has no reorder buffer and no expected-sequence
// counter, by design.

// installOrderedDelivery replaces c.accept. The state below lives in the
// closure and is touched only by the connection's mainLoop.
func installOrderedDelivery(c *conn) {
	next := uint32(1)            // lowest sequence number not yet delivered
	held := map[uint32][]byte{}  // arrived early, waiting for the gap to fill
	present := map[uint32]bool{} // distinguishes "held a nil payload" from "absent"

	c.accept = func(seq uint32, payload []byte) {
		if seq < next || present[seq] {
			return // already delivered or already held: in-order mode de-duplicates
		}
		held[seq], present[seq] = payload, true
		for present[next] {
			c.deliverQ = append(c.deliverQ, delivery{h: c.handle, data: held[next]})
			delete(held, next)
			delete(present, next)
			next++
		}
	}
}
