package rudp

import "encoding/binary"

// Packet types. See docs/wire-format.md.
const (
	typeConnect = 1
	typeConnAck = 2
	typeData    = 3
	typeAck     = 4
	typeClose   = 5
)

const (
	headerLen = 11 // type(1) connID(4) seq(4) payloadLen(2)

	// MaxPayload is the largest message Write accepts.
	MaxPayload = 1200

	maxPacket = headerLen + MaxPayload
)

type packet struct {
	typ     uint8
	connID  uint32
	seq     uint32
	payload []byte
}

// marshal encodes one packet: big-endian, fixed 11-byte header, then payload.
func marshal(typ uint8, connID, seq uint32, payload []byte) []byte {
	b := make([]byte, headerLen+len(payload))
	b[0] = typ
	binary.BigEndian.PutUint32(b[1:5], connID)
	binary.BigEndian.PutUint32(b[5:9], seq)
	binary.BigEndian.PutUint16(b[9:11], uint16(len(payload)))
	copy(b[headerLen:], payload)
	return b
}

// parse decodes one datagram. It rejects anything whose declared payload
// length disagrees with the datagram size. The payload is copied, so the
// caller may reuse b.
func parse(b []byte) (packet, bool) {
	if len(b) < headerLen {
		return packet{}, false
	}
	p := packet{
		typ:    b[0],
		connID: binary.BigEndian.Uint32(b[1:5]),
		seq:    binary.BigEndian.Uint32(b[5:9]),
	}
	if p.typ < typeConnect || p.typ > typeClose {
		return packet{}, false
	}
	n := int(binary.BigEndian.Uint16(b[9:11]))
	if n != len(b)-headerLen || n > MaxPayload {
		return packet{}, false
	}
	if n > 0 {
		p.payload = append([]byte(nil), b[headerLen:]...)
	}
	return p, true
}
