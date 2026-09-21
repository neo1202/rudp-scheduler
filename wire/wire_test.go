package wire

import (
	"bytes"
	"errors"
	"math"
	"strings"
	"testing"
	"testing/quick"
)

func TestRoundTrip(t *testing.T) {
	msgs := []Message{
		Join{},
		Request{Lo: 0, Hi: math.MaxUint64, Msg: ""},
		Request{Lo: 17, Hi: 99, Msg: "hello world"},
		Request{Lo: 1, Hi: 2, Msg: strings.Repeat("x", MaxMsgLen)},
		Chunk{ID: TaskID{Client: 7, Idx: 3}, Lo: 10000, Hi: 20000, Msg: "héllo"},
		Chunk{ID: TaskID{Client: math.MaxUint32, Idx: math.MaxUint32}, Lo: 1, Hi: 2, Msg: strings.Repeat("y", MaxMsgLen)},
		ChunkResult{ID: TaskID{Client: 7, Idx: 3}, Hash: 0xDEADBEEFCAFEF00D, Nonce: 12345},
		Result{Hash: 1, Nonce: math.MaxUint64},
	}
	for _, want := range msgs {
		got, err := Decode(Encode(want))
		if err != nil {
			t.Errorf("%T: %v", want, err)
			continue
		}
		if got != want {
			t.Errorf("round trip: got %#v, want %#v", got, want)
		}
	}
}

func TestLayouts(t *testing.T) {
	cases := []struct {
		m    Message
		want []byte
	}{
		{Join{}, []byte{1}},
		{Request{Lo: 1, Hi: 2, Msg: "ab"},
			[]byte{2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 2, 'a', 'b'}},
		{Chunk{ID: TaskID{Client: 0x01020304, Idx: 5}, Lo: 1, Hi: 2, Msg: "z"},
			[]byte{3, 1, 2, 3, 4, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 1, 'z'}},
		{ChunkResult{ID: TaskID{Client: 1, Idx: 2}, Hash: 3, Nonce: 4},
			[]byte{4, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 4}},
		{Result{Hash: 0x0102030405060708, Nonce: 9},
			[]byte{5, 1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 0, 0, 0, 0, 0, 9}},
	}
	for _, c := range cases {
		if got := Encode(c.m); !bytes.Equal(got, c.want) {
			t.Errorf("%T layout = % x, want % x", c.m, got, c.want)
		}
	}
}

func TestLargestMessageFitsOneTransportPayload(t *testing.T) {
	const transportMaxPayload = 1200 // rudp.MaxPayload; wire does not import the transport
	big := Encode(Chunk{Msg: strings.Repeat("m", MaxMsgLen)})
	if len(big) > transportMaxPayload {
		t.Errorf("largest Chunk is %d bytes, transport carries at most %d", len(big), transportMaxPayload)
	}
}

func TestDecodeRejectsMalformed(t *testing.T) {
	req := Encode(Request{Lo: 1, Hi: 2, Msg: "abc"})
	lying := append([]byte(nil), req...)
	lying[18] = 200 // msgLen claims more than is there
	tooLong := Encode(Request{Msg: strings.Repeat("x", MaxMsgLen)})
	tooLong = append(tooLong, 'x')
	tooLong[17], tooLong[18] = byte((MaxMsgLen+1)>>8), byte((MaxMsgLen+1)&0xFF)

	cases := map[string][]byte{
		"empty":                  {},
		"unknown kind 0":         {0},
		"unknown kind 6":         {6},
		"join with trailing":     {1, 0},
		"request truncated":      req[:len(req)-1],
		"request trailing":       append(append([]byte(nil), req...), 0),
		"request header only":    req[:10],
		"request lying length":   lying,
		"request msg too long":   tooLong,
		"chunk truncated":        Encode(Chunk{Msg: "abc"})[:26],
		"chunkresult short":      Encode(ChunkResult{})[:24],
		"chunkresult trailing":   append(Encode(ChunkResult{}), 0),
		"result short":           Encode(Result{})[:16],
		"result trailing":        append(Encode(Result{}), 0),
		"result kind, join size": {5},
	}
	for name, b := range cases {
		if m, err := Decode(b); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: Decode = (%#v, %v), want ErrMalformed", name, m, err)
		}
	}
}

func TestDecodeNeverPanics(t *testing.T) {
	f := func(b []byte) bool {
		m, err := Decode(b)
		return (m == nil) == (err != nil)
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 20000}); err != nil {
		t.Error(err)
	}
}

func TestEncodePanicsOnOversizedMsg(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("Encode accepted a message string longer than MaxMsgLen")
		}
	}()
	Encode(Request{Msg: strings.Repeat("x", MaxMsgLen+1)})
}
