package wal

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

var sample = []Record{
	JobStart{Job: 7, Lo: 10, Hi: 1_000_010, Size: 10_000, N: 100, Msg: "hello wal"},
	ChunkDone{Job: 7, Idx: 3, Hash: 0xDEADBEEF, Nonce: 31_337},
	ChunkDone{Job: 7, Idx: 0, Hash: 42, Nonce: 17},
	JobStart{Job: 8, Lo: 0, Hi: 1, Size: 1, N: 1, Msg: strings.Repeat("m", 1024)},
	JobDone{Job: 7, Hash: 42, Nonce: 17},
	JobDrop{Job: 8},
}

// same compares record lists, treating nil and empty alike.
func same(a, b []Record) bool {
	return (len(a) == 0 && len(b) == 0) || reflect.DeepEqual(a, b)
}

func write(t *testing.T, path string, recs []Record) {
	t.Helper()
	l, got, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("fresh log returned %d records", len(got))
	}
	for _, r := range recs {
		if err := l.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}

func reopen(t *testing.T, path string) (*Log, []Record) {
	t.Helper()
	l, recs, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	return l, recs
}

func TestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	write(t, path, sample)
	l, got := reopen(t, path)
	defer l.Close()
	if !reflect.DeepEqual(got, sample) {
		t.Errorf("replayed %#v\nwant     %#v", got, sample)
	}
}

// A crash can cut the file anywhere. Whatever the cut, Open must return a
// prefix of what was written, and appending afterwards must work.
func TestTornTailAtEveryOffset(t *testing.T) {
	dir := t.TempDir()
	full := filepath.Join(dir, "full")
	write(t, full, sample)
	data, err := os.ReadFile(full)
	if err != nil {
		t.Fatal(err)
	}

	for cut := 0; cut <= len(data); cut++ {
		path := filepath.Join(dir, "torn")
		if err := os.WriteFile(path, data[:cut], 0o644); err != nil {
			t.Fatal(err)
		}
		l, got := reopen(t, path)
		if len(got) > len(sample) || !same(got, sample[:len(got)]) {
			t.Fatalf("cut at %d: replay is not a prefix: %#v", cut, got)
		}
		if cut == len(data) && len(got) != len(sample) {
			t.Fatalf("uncut file lost records: %d of %d", len(got), len(sample))
		}
		extra := JobDrop{Job: 99}
		if err := l.Append(extra); err != nil {
			t.Fatal(err)
		}
		l.Close()
		_, again := reopen(t, path)
		if want := append(append([]Record(nil), got...), extra); !same(again, want) {
			t.Fatalf("cut at %d: after append replay = %#v, want %#v", cut, again, want)
		}
	}
}

func TestCorruptionStopsReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	write(t, path, sample)
	data, _ := os.ReadFile(path)
	// Flip a byte inside the second record's body.
	first := frameHeader + len(encode(sample[0]))
	data[first+frameHeader+5] ^= 0xFF
	os.WriteFile(path, data, 0o644)

	l, got := reopen(t, path)
	defer l.Close()
	if !reflect.DeepEqual(got, sample[:1]) {
		t.Errorf("replay past a corrupt record: %#v", got)
	}
}

func TestRewriteCompacts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	write(t, path, sample)
	l, _ := reopen(t, path)
	before := l.Size()

	keep := []Record{sample[4]}
	if err := l.Rewrite(keep); err != nil {
		t.Fatal(err)
	}
	if l.Size() >= before {
		t.Errorf("rewrite did not shrink the log: %d -> %d", before, l.Size())
	}
	more := JobStart{Job: 9, N: 1, Size: 1, Hi: 1, Msg: "after"}
	if err := l.Append(more); err != nil {
		t.Fatal(err)
	}
	l.Close()

	_, got := reopen(t, path)
	if want := append(keep, more); !reflect.DeepEqual(got, want) {
		t.Errorf("after rewrite replay = %#v, want %#v", got, want)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Error("temporary file left behind")
	}
}
