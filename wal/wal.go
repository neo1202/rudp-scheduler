// Package wal is the scheduler's write-ahead log: an append-only file of
// framed, checksummed records from which the aggregator rebuilds its jobs
// after a restart.
//
// A Log is not safe for concurrent use and does not need to be. It has one
// owner, the aggregator goroutine, like every other piece of state here.
//
// Durability is an optimisation, not a correctness requirement. Every record
// describes work that can be redone: a lost ChunkDone means that chunk is
// computed again and merged again, which changes nothing because Merge is
// idempotent and Compute is deterministic; a lost JobStart is repaired by the
// client resubmitting. So records are buffered and synced in groups, and a
// torn tail is simply cut off on the next Open.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// Record is one of JobStart, ChunkDone, JobDone, JobDrop.
type Record interface{ typ() byte }

// JobStart registers a job. Size is the chunk width that was actually used,
// so a recovered job is split exactly as before even if the server's
// configuration has changed since.
type JobStart struct {
	Job    uint64
	Lo, Hi uint64
	Size   uint64
	N      uint32
	Msg    string
}

// ChunkDone records the counted result of one task.
type ChunkDone struct {
	Job   uint64
	Idx   uint32
	Hash  uint64
	Nonce uint64
}

// JobDone records a job's final answer.
type JobDone struct {
	Job   uint64
	Hash  uint64
	Nonce uint64
}

// JobDrop records that a job, running or finished, has been forgotten.
type JobDrop struct{ Job uint64 }

func (JobStart) typ() byte  { return 1 }
func (ChunkDone) typ() byte { return 2 }
func (JobDone) typ() byte   { return 3 }
func (JobDrop) typ() byte   { return 4 }

const (
	frameHeader = 8       // bodyLen(4) crc32(4)
	maxBody     = 1 << 16 // far above the largest record; rejects garbage lengths
)

// Log is an open write-ahead log.
type Log struct {
	path string
	f    *os.File
	w    *bufio.Writer
	size int64
}

// Open opens (or creates) the log at path and returns every intact record in
// it. Reading stops at the first frame that is truncated or fails its
// checksum, which is what a crash in the middle of a write leaves behind; the
// file is cut there so that new records follow the last good one.
func Open(path string) (*Log, []Record, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, nil, err
	}
	recs, good, err := readAll(bufio.NewReader(f))
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	if err := f.Truncate(good); err != nil {
		f.Close()
		return nil, nil, err
	}
	if _, err := f.Seek(good, io.SeekStart); err != nil {
		f.Close()
		return nil, nil, err
	}
	return &Log{path: path, f: f, w: bufio.NewWriter(f), size: good}, recs, nil
}

// readAll returns the intact records and the offset just past the last one.
func readAll(r io.Reader) ([]Record, int64, error) {
	var (
		recs []Record
		good int64
		head [frameHeader]byte
	)
	for {
		if _, err := io.ReadFull(r, head[:]); err != nil {
			return recs, good, nil // clean end, or a torn header
		}
		n := binary.BigEndian.Uint32(head[0:4])
		if n == 0 || n > maxBody {
			return recs, good, nil
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(r, body); err != nil {
			return recs, good, nil
		}
		if crc32.ChecksumIEEE(body) != binary.BigEndian.Uint32(head[4:8]) {
			return recs, good, nil
		}
		rec, ok := decode(body)
		if !ok {
			return recs, good, nil
		}
		recs = append(recs, rec)
		good += frameHeader + int64(n)
	}
}

// Append buffers one record. Call Sync to make it durable.
func (l *Log) Append(r Record) error {
	body := encode(r)
	var head [frameHeader]byte
	binary.BigEndian.PutUint32(head[0:4], uint32(len(body)))
	binary.BigEndian.PutUint32(head[4:8], crc32.ChecksumIEEE(body))
	if _, err := l.w.Write(head[:]); err != nil {
		return err
	}
	if _, err := l.w.Write(body); err != nil {
		return err
	}
	l.size += frameHeader + int64(len(body))
	return nil
}

// Sync flushes buffered records and forces them to stable storage.
func (l *Log) Sync() error {
	if err := l.w.Flush(); err != nil {
		return err
	}
	return l.f.Sync()
}

// Size returns the log's length in bytes, buffered records included.
func (l *Log) Size() int64 { return l.size }

// Rewrite atomically replaces the log's contents with recs. It is how the log
// is compacted: the owner passes the minimal set of records that describes
// its current state. A crash at any point leaves either the old file or the
// new one, never a mixture.
func (l *Log) Rewrite(recs []Record) error {
	tmp := l.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	next := &Log{path: l.path, f: f, w: bufio.NewWriter(f)}
	for _, r := range recs {
		if err := next.Append(r); err != nil {
			f.Close()
			return err
		}
	}
	if err := next.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := os.Rename(tmp, l.path); err != nil {
		f.Close()
		return err
	}
	if dir, err := os.Open(filepath.Dir(l.path)); err == nil {
		dir.Sync() // make the rename itself durable
		dir.Close()
	}
	l.f.Close()
	*l = *next
	return nil
}

// Close syncs and closes the log.
func (l *Log) Close() error {
	err := l.Sync()
	return errors.Join(err, l.f.Close())
}

func encode(r Record) []byte {
	be := binary.BigEndian
	switch r := r.(type) {
	case JobStart:
		b := make([]byte, 39, 39+len(r.Msg))
		b[0] = r.typ()
		be.PutUint64(b[1:], r.Job)
		be.PutUint64(b[9:], r.Lo)
		be.PutUint64(b[17:], r.Hi)
		be.PutUint64(b[25:], r.Size)
		be.PutUint32(b[33:], r.N)
		be.PutUint16(b[37:], uint16(len(r.Msg)))
		return append(b, r.Msg...)
	case ChunkDone:
		b := make([]byte, 29)
		b[0] = r.typ()
		be.PutUint64(b[1:], r.Job)
		be.PutUint32(b[9:], r.Idx)
		be.PutUint64(b[13:], r.Hash)
		be.PutUint64(b[21:], r.Nonce)
		return b
	case JobDone:
		b := make([]byte, 25)
		b[0] = r.typ()
		be.PutUint64(b[1:], r.Job)
		be.PutUint64(b[9:], r.Hash)
		be.PutUint64(b[17:], r.Nonce)
		return b
	case JobDrop:
		b := make([]byte, 9)
		b[0] = r.typ()
		be.PutUint64(b[1:], r.Job)
		return b
	}
	panic(fmt.Sprintf("wal: unknown record type %T", r))
}

func decode(b []byte) (Record, bool) {
	be := binary.BigEndian
	switch b[0] {
	case 1:
		if len(b) < 39 || len(b) != 39+int(be.Uint16(b[37:])) {
			return nil, false
		}
		return JobStart{Job: be.Uint64(b[1:]), Lo: be.Uint64(b[9:]), Hi: be.Uint64(b[17:]),
			Size: be.Uint64(b[25:]), N: be.Uint32(b[33:]), Msg: string(b[39:])}, true
	case 2:
		if len(b) != 29 {
			return nil, false
		}
		return ChunkDone{Job: be.Uint64(b[1:]), Idx: be.Uint32(b[9:]), Hash: be.Uint64(b[13:]), Nonce: be.Uint64(b[21:])}, true
	case 3:
		if len(b) != 25 {
			return nil, false
		}
		return JobDone{Job: be.Uint64(b[1:]), Hash: be.Uint64(b[9:]), Nonce: be.Uint64(b[17:])}, true
	case 4:
		if len(b) != 9 {
			return nil, false
		}
		return JobDrop{Job: be.Uint64(b[1:])}, true
	}
	return nil, false
}
