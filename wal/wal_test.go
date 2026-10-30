package wal

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func openT(t *testing.T, path string) (*Log, [][]byte, RecoveryInfo) {
	t.Helper()
	var got [][]byte
	l, info, err := Open(path, Options{}, func(p []byte) error {
		got = append(got, append([]byte(nil), p...))
		return nil
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return l, got, info
}

func record(i int) []byte {
	return []byte(fmt.Sprintf("record-%04d-%s", i, bytes.Repeat([]byte{'x'}, i%37)))
}

func TestAppendReplayRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	l, got, _ := openT(t, path)
	if len(got) != 0 {
		t.Fatalf("fresh log replayed %d records", len(got))
	}
	for i := 0; i < 500; i++ {
		if err := l.Append(record(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l, got, info := openT(t, path)
	defer l.Close()
	if info.Records != 500 || info.TruncatedBytes != 0 {
		t.Fatalf("info = %+v", info)
	}
	for i, p := range got {
		if !bytes.Equal(p, record(i)) {
			t.Fatalf("record %d = %q", i, p)
		}
	}
}

func TestEmptyPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	l, _, _ := openT(t, path)
	if err := l.Append(nil); err != nil {
		t.Fatal(err)
	}
	l.Close()
	l, got, _ := openT(t, path)
	defer l.Close()
	if len(got) != 1 || len(got[0]) != 0 {
		t.Fatalf("got %q", got)
	}
}

// Truncating the file at every possible byte offset simulates a crash at any
// point during a write. Recovery must always yield an exact prefix of the
// records, and appends afterwards must be readable.
func TestTornWriteAtEveryOffset(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	l, _, _ := openT(t, src)
	var ends []int64
	for i := 0; i < 20; i++ {
		if err := l.Append(record(i)); err != nil {
			t.Fatal(err)
		}
		ends = append(ends, l.Size())
	}
	l.Close()
	full, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	for cut := 0; cut <= len(full); cut++ {
		path := filepath.Join(dir, fmt.Sprintf("cut-%d", cut))
		if err := os.WriteFile(path, full[:cut], 0o644); err != nil {
			t.Fatal(err)
		}
		want := 0
		for want < len(ends) && ends[want] <= int64(cut) {
			want++
		}
		l, got, info := openT(t, path)
		if len(got) != want {
			t.Fatalf("cut %d: replayed %d records, want %d", cut, len(got), want)
		}
		if err := l.Append([]byte("after-crash")); err != nil {
			t.Fatal(err)
		}
		l.Close()
		l, got, _ = openT(t, path)
		l.Close()
		if len(got) != want+1 || string(got[want]) != "after-crash" {
			t.Fatalf("cut %d: append after recovery lost (info %+v, got %d)", cut, info, len(got))
		}
	}
}

func TestCorruptionStopsReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	l, _, _ := openT(t, path)
	var ends []int64
	for i := 0; i < 10; i++ {
		l.Append(record(i))
		ends = append(ends, l.Size())
	}
	l.Close()
	data, _ := os.ReadFile(path)
	data[ends[4]+headerSize+2] ^= 0xFF // flip a byte in record 5's payload
	os.WriteFile(path, data, 0o644)

	l, got, info := openT(t, path)
	defer l.Close()
	if len(got) != 5 {
		t.Fatalf("replayed %d records, want 5", len(got))
	}
	if info.ValidBytes != ends[4] || info.TruncatedBytes != int64(len(data))-ends[4] {
		t.Fatalf("info = %+v", info)
	}
}

func TestRandomGarbageNeverPanics(t *testing.T) {
	dir := t.TempDir()
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 200; i++ {
		buf := make([]byte, rng.Intn(4096))
		rng.Read(buf)
		path := filepath.Join(dir, fmt.Sprintf("g%d", i))
		os.WriteFile(path, buf, 0o644)
		l, _, _ := openT(t, path)
		l.Close()
	}
}

func TestConcurrentAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	l, _, _ := openT(t, path)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if err := l.Append([]byte(fmt.Sprintf("g%d-%d", g, i))); err != nil {
					t.Error(err)
				}
			}
		}(g)
	}
	wg.Wait()
	l.Close()
	l, got, _ := openT(t, path)
	defer l.Close()
	if len(got) != 1600 {
		t.Fatalf("got %d records", len(got))
	}
}

func TestClosedAndTooLarge(t *testing.T) {
	l, _, _ := openT(t, filepath.Join(t.TempDir(), "wal"))
	if err := l.Append(make([]byte, MaxRecordSize+1)); err != ErrTooLarge {
		t.Fatalf("err = %v", err)
	}
	l.Close()
	if err := l.Append([]byte("x")); err != ErrClosed {
		t.Fatalf("err = %v", err)
	}
}

func BenchmarkAppend(b *testing.B) {
	l, _, err := Open(filepath.Join(b.TempDir(), "wal"), Options{}, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()
	p := bytes.Repeat([]byte{'v'}, 128)
	b.SetBytes(int64(len(p)))
	for i := 0; i < b.N; i++ {
		l.Append(p)
	}
}
