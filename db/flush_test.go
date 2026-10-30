package db

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func openSmall(t *testing.T, dir string, memSize int64) *DB {
	t.Helper()
	d, err := Open(dir, Options{NoSync: true, MemtableSize: memSize})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func glob(t *testing.T, dir, pat string) []string {
	t.Helper()
	m, err := filepath.Glob(filepath.Join(dir, pat))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestFlushCreatesL0AndRotatesWAL(t *testing.T) {
	dir := t.TempDir()
	d := openSmall(t, dir, 4<<10)
	for i := 0; i < 500; i++ {
		if err := d.Put([]byte(fmt.Sprintf("key%04d", i)), []byte("value-value-value")); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(glob(t, dir, "*.sst")); n < 2 {
		t.Fatalf("expected several L0 tables, got %d", n)
	}
	if n := len(glob(t, dir, "*.log")); n != 1 {
		t.Fatalf("old WAL segments not deleted: %v", glob(t, dir, "*.log"))
	}
	for i := 0; i < 500; i++ {
		if v, err := d.Get([]byte(fmt.Sprintf("key%04d", i))); err != nil || string(v) != "value-value-value" {
			t.Fatalf("key%04d: %q %v", i, v, err)
		}
	}
	d.Close()

	d = openSmall(t, dir, 4<<10)
	defer d.Close()
	for i := 0; i < 500; i++ {
		if _, err := d.Get([]byte(fmt.Sprintf("key%04d", i))); err != nil {
			t.Fatalf("after reopen key%04d: %v", i, err)
		}
	}
	// Sequence numbers must continue past flushed data so new writes win.
	d.Put([]byte("key0000"), []byte("new"))
	if v, _ := d.Get([]byte("key0000")); string(v) != "new" {
		t.Fatalf("stale read after reopen: %q", v)
	}
}

func TestTombstoneShadowsFlushedValue(t *testing.T) {
	dir := t.TempDir()
	d := openSmall(t, dir, 1<<20)
	d.Put([]byte("a"), []byte("1"))
	d.Put([]byte("b"), []byte("1"))
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	d.Delete([]byte("a"))
	if _, err := d.Get([]byte("a")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("memtable tombstone ignored: %v", err)
	}
	d.Flush() // tombstone now in a newer L0 table over an older value
	if _, err := d.Get([]byte("a")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("L0 tombstone ignored: %v", err)
	}
	d.Put([]byte("b"), []byte("2"))
	d.Flush()
	d.Close()
	d = openSmall(t, dir, 1<<20)
	defer d.Close()
	if _, err := d.Get([]byte("a")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("tombstone lost on reopen: %v", err)
	}
	if v, _ := d.Get([]byte("b")); string(v) != "2" {
		t.Fatalf("b = %q", v)
	}
}

func TestFlushEmptyIsNoop(t *testing.T) {
	dir := t.TempDir()
	d := openSmall(t, dir, 0)
	defer d.Close()
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	if n := len(glob(t, dir, "*.sst")); n != 0 {
		t.Fatalf("empty flush produced %d tables", n)
	}
}

// A crash after the table is written but before the MANIFEST edit must leave
// the old WAL authoritative and the orphan table ignored.
func TestCrashBeforeManifestCommit(t *testing.T) {
	dir := t.TempDir()
	d := openSmall(t, dir, 1<<20)
	for i := 0; i < 50; i++ {
		d.Put([]byte(fmt.Sprintf("k%02d", i)), []byte("v"))
	}
	d.Sync()
	// Simulate the crashed flush's leftovers: orphan table, empty next WAL, tmp file.
	orphan := sstPath(dir, d.next+1)
	if _, err := d.writeTable(d.next + 1); err != nil {
		t.Fatal(err)
	}
	newWAL := walPath(dir, d.next)
	os.WriteFile(newWAL, nil, 0o644)
	os.WriteFile(sstPath(dir, 999)+".tmp", []byte("junk"), 0o644)
	d.Close()

	d = openSmall(t, dir, 1<<20)
	defer d.Close()
	for i := 0; i < 50; i++ {
		if _, err := d.Get([]byte(fmt.Sprintf("k%02d", i))); err != nil {
			t.Fatalf("k%02d lost: %v", i, err)
		}
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatal("orphan table not removed")
	}
	if len(glob(t, dir, "*.tmp")) != 0 {
		t.Fatal("tmp file not removed")
	}
	d.Put([]byte("after"), []byte("1"))
	if _, err := d.Get([]byte("after")); err != nil {
		t.Fatal(err)
	}
}

// A crash after the MANIFEST commit but before the old WAL was deleted: the
// stale segment must not be replayed over newer flushed data.
func TestCrashAfterManifestCommitKeepsStaleWAL(t *testing.T) {
	dir := t.TempDir()
	d := openSmall(t, dir, 1<<20)
	d.Put([]byte("a"), []byte("old"))
	oldWAL := walPath(dir, d.logNum)
	stale, _ := os.ReadFile(oldWAL)
	d.Flush()
	d.Put([]byte("a"), []byte("new"))
	d.Flush()
	d.Close()
	os.WriteFile(oldWAL, stale, 0o644) // resurrect the deleted segment

	d = openSmall(t, dir, 1<<20)
	defer d.Close()
	if v, _ := d.Get([]byte("a")); string(v) != "new" {
		t.Fatalf("stale WAL replayed: a = %q", v)
	}
	if _, err := os.Stat(oldWAL); !os.IsNotExist(err) {
		t.Fatal("covered WAL segment not removed on open")
	}
}

func TestCorruptTableFailsOpen(t *testing.T) {
	dir := t.TempDir()
	d := openSmall(t, dir, 1<<20)
	d.Put([]byte("a"), []byte("1"))
	d.Flush()
	d.Close()
	p := glob(t, dir, "*.sst")[0]
	b, _ := os.ReadFile(p)
	b[len(b)-3] ^= 0xff
	os.WriteFile(p, b, 0o644)
	if _, err := Open(dir, Options{}); err == nil {
		t.Fatal("opened with corrupt table")
	}
}

func TestRandomModelWithFlushesAndReopens(t *testing.T) {
	dir := t.TempDir()
	rng := rand.New(rand.NewSource(7))
	d := openSmall(t, dir, 8<<10)
	model := map[string]string{}
	for i := 0; i < 30000; i++ {
		k := fmt.Sprintf("k%03d", rng.Intn(400))
		switch r := rng.Intn(10); {
		case r < 6:
			v := fmt.Sprintf("v%d", i)
			d.Put([]byte(k), []byte(v))
			model[k] = v
		case r < 8:
			d.Delete([]byte(k))
			delete(model, k)
		default:
			v, err := d.Get([]byte(k))
			want, ok := model[k]
			if ok && (err != nil || string(v) != want) || !ok && !errors.Is(err, ErrNotFound) {
				t.Fatalf("op %d key %s: got %q %v want %q (%v)", i, k, v, err, want, ok)
			}
		}
		if i%3000 == 2999 {
			d.Close()
			d = openSmall(t, dir, 8<<10)
		}
	}
	defer d.Close()
	for j := 0; j < 400; j++ {
		k := fmt.Sprintf("k%03d", j)
		v, err := d.Get([]byte(k))
		want, ok := model[k]
		if ok && (err != nil || string(v) != want) || !ok && !errors.Is(err, ErrNotFound) {
			t.Fatalf("final %s: got %q %v want %q (%v)", k, v, err, want, ok)
		}
	}
	if len(glob(t, dir, "*.sst")) == 0 {
		t.Fatal("test never flushed")
	}
}
