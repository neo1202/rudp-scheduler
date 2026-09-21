package rudp

import (
	"bytes"
	"testing"
)

func TestPacketRoundTrip(t *testing.T) {
	cases := []packet{
		{typ: typeConnect},
		{typ: typeConnAck, connID: 7},
		{typ: typeData, connID: 0xDEADBEEF, seq: 1, payload: []byte("hello")},
		{typ: typeData, connID: 1, seq: 0xFFFFFFFF, payload: bytes.Repeat([]byte{0xAB}, MaxPayload)},
		{typ: typeAck, connID: 3, seq: 42},
		{typ: typeClose, connID: 3},
	}
	for _, want := range cases {
		b := marshal(want.typ, want.connID, want.seq, want.payload)
		if len(b) != headerLen+len(want.payload) {
			t.Fatalf("type %d: encoded length %d, want %d", want.typ, len(b), headerLen+len(want.payload))
		}
		got, ok := parse(b)
		if !ok {
			t.Fatalf("type %d: parse rejected a valid packet", want.typ)
		}
		if got.typ != want.typ || got.connID != want.connID || got.seq != want.seq || !bytes.Equal(got.payload, want.payload) {
			t.Errorf("round trip: got %+v, want %+v", got, want)
		}
	}
}

func TestHeaderLayoutIsBigEndian(t *testing.T) {
	got := marshal(typeData, 0x01020304, 0x0A0B0C0D, []byte{0xFF, 0xEE})
	want := []byte{3, 1, 2, 3, 4, 0x0A, 0x0B, 0x0C, 0x0D, 0, 2, 0xFF, 0xEE}
	if !bytes.Equal(got, want) {
		t.Errorf("layout = % x, want % x", got, want)
	}
}

func TestParseRejectsMalformed(t *testing.T) {
	valid := marshal(typeData, 1, 1, []byte("abc"))
	cases := map[string][]byte{
		"empty":             {},
		"short header":      valid[:headerLen-1],
		"truncated payload": valid[:len(valid)-1],
		"trailing byte":     append(append([]byte(nil), valid...), 0),
		"type zero":         append([]byte{0}, valid[1:]...),
		"type too large":    append([]byte{6}, valid[1:]...),
		"oversized payload": marshal(typeData, 1, 1, make([]byte, MaxPayload+1)),
	}
	for name, b := range cases {
		if _, ok := parse(b); ok {
			t.Errorf("%s: parse accepted a malformed packet", name)
		}
	}
}

func TestParseCopiesPayload(t *testing.T) {
	b := marshal(typeData, 1, 1, []byte("abc"))
	p, _ := parse(b)
	b[headerLen] = 'X'
	if string(p.payload) != "abc" {
		t.Errorf("payload aliases the read buffer: %q", p.payload)
	}
}
