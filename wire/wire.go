// Package wire encodes the application messages exchanged between client,
// server and worker. Every message is a fixed big-endian layout; the
// authoritative description is docs/wire-format.md.
//
// The transport never looks inside these bytes, and nothing here knows about
// sequence numbers or acknowledgements.
package wire

import (
	"encoding/binary"
	"errors"
)

// Message kinds (first byte of every message).
const (
	KindJoin        = 1
	KindRequest     = 2
	KindChunk       = 3
	KindChunkResult = 4
	KindResult      = 5
)

// MaxMsgLen bounds the job message so that the largest message (Chunk) fits
// into one transport payload with room to spare.
const MaxMsgLen = 1024

// ErrMalformed is returned by Decode for anything that is not exactly one
// well-formed message.
var ErrMalformed = errors.New("wire: malformed message")

// TaskID names one chunk of one job: the job's ID and the chunk's index within
// it. It is the idempotency key of the whole system.
//
// The job ID is chosen by the client, not derived from its connection, so a
// job keeps its identity when the client reconnects or the server restarts.
type TaskID struct {
	Job uint64
	Idx uint32
}

// Message is one of Join, Request, Chunk, ChunkResult, Result.
type Message interface{ kind() byte }

// Join is a worker's first message: "I compute, I do not submit".
type Join struct{}

// Request asks for the best result over the range [Lo, Hi) of job Msg.
// Sending the same Job again is harmless: it re-attaches to the running job,
// or fetches the stored answer if the job has already finished.
type Request struct {
	Job    uint64
	Lo, Hi uint64
	Msg    string
}

// Chunk is one task handed to a worker.
type Chunk struct {
	ID     TaskID
	Lo, Hi uint64
	Msg    string
}

// ChunkResult is a worker's answer for one Chunk.
type ChunkResult struct {
	ID    TaskID
	Hash  uint64
	Nonce uint64
}

// Result is the answer to a Request.
type Result struct {
	Job   uint64
	Hash  uint64
	Nonce uint64
}

func (Join) kind() byte        { return KindJoin }
func (Request) kind() byte     { return KindRequest }
func (Chunk) kind() byte       { return KindChunk }
func (ChunkResult) kind() byte { return KindChunkResult }
func (Result) kind() byte      { return KindResult }

// Encode serialises m. It panics if a message string exceeds MaxMsgLen, which
// is a programming error: callers validate user input first.
func Encode(m Message) []byte {
	be := binary.BigEndian
	switch m := m.(type) {
	case Join:
		return []byte{KindJoin}
	case Request:
		b := make([]byte, 27, 27+len(m.Msg))
		b[0] = KindRequest
		be.PutUint64(b[1:], m.Job)
		be.PutUint64(b[9:], m.Lo)
		be.PutUint64(b[17:], m.Hi)
		be.PutUint16(b[25:], msgLen(m.Msg))
		return append(b, m.Msg...)
	case Chunk:
		b := make([]byte, 31, 31+len(m.Msg))
		b[0] = KindChunk
		be.PutUint64(b[1:], m.ID.Job)
		be.PutUint32(b[9:], m.ID.Idx)
		be.PutUint64(b[13:], m.Lo)
		be.PutUint64(b[21:], m.Hi)
		be.PutUint16(b[29:], msgLen(m.Msg))
		return append(b, m.Msg...)
	case ChunkResult:
		b := make([]byte, 29)
		b[0] = KindChunkResult
		be.PutUint64(b[1:], m.ID.Job)
		be.PutUint32(b[9:], m.ID.Idx)
		be.PutUint64(b[13:], m.Hash)
		be.PutUint64(b[21:], m.Nonce)
		return b
	case Result:
		b := make([]byte, 25)
		b[0] = KindResult
		be.PutUint64(b[1:], m.Job)
		be.PutUint64(b[9:], m.Hash)
		be.PutUint64(b[17:], m.Nonce)
		return b
	}
	panic("wire: unknown message type")
}

func msgLen(s string) uint16 {
	if len(s) > MaxMsgLen {
		panic("wire: message string exceeds MaxMsgLen")
	}
	return uint16(len(s))
}

// Decode parses exactly one message. Truncated input, trailing bytes, an
// unknown kind, or a string length that disagrees with the input all yield
// ErrMalformed.
func Decode(b []byte) (Message, error) {
	if len(b) == 0 {
		return nil, ErrMalformed
	}
	be := binary.BigEndian
	switch b[0] {
	case KindJoin:
		if len(b) == 1 {
			return Join{}, nil
		}
	case KindRequest:
		if s, ok := tail(b, 25); ok {
			return Request{Job: be.Uint64(b[1:]), Lo: be.Uint64(b[9:]), Hi: be.Uint64(b[17:]), Msg: s}, nil
		}
	case KindChunk:
		if s, ok := tail(b, 29); ok {
			return Chunk{
				ID: TaskID{Job: be.Uint64(b[1:]), Idx: be.Uint32(b[9:])},
				Lo: be.Uint64(b[13:]), Hi: be.Uint64(b[21:]), Msg: s,
			}, nil
		}
	case KindChunkResult:
		if len(b) == 29 {
			return ChunkResult{
				ID:   TaskID{Job: be.Uint64(b[1:]), Idx: be.Uint32(b[9:])},
				Hash: be.Uint64(b[13:]), Nonce: be.Uint64(b[21:]),
			}, nil
		}
	case KindResult:
		if len(b) == 25 {
			return Result{Job: be.Uint64(b[1:]), Hash: be.Uint64(b[9:]), Nonce: be.Uint64(b[17:])}, nil
		}
	}
	return nil, ErrMalformed
}

// tail reads the length-prefixed string whose 2-byte length sits at b[at:],
// and requires it to end exactly at the end of b.
func tail(b []byte, at int) (string, bool) {
	if len(b) < at+2 {
		return "", false
	}
	n := int(binary.BigEndian.Uint16(b[at:]))
	if n > MaxMsgLen || len(b) != at+2+n {
		return "", false
	}
	return string(b[at+2:]), true
}
