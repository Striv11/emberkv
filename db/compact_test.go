package db

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"testing"
)

func smallOpts() Options {
	return Options{NoSync: true, MemtableSize: 2 << 10, L0Trigger: 3, L1Size: 8 << 10, TargetFileSize: 3 << 10}
}

func checkLevelInvariants(t *testing.T, d *DB) {
	t.Helper()
	d.mu.RLock()
	defer d.mu.RUnlock()
	for l := 1; l < numLevels; l++ {
		fs := d.levels[l]
		for i := 1; i < len(fs); i++ {
			if bytes.Compare(d.meta[fs[i-1]].Largest, d.meta[fs[i]].Smallest) >= 0 {
				t.Fatalf("level %d tables %d and %d overlap", l, fs[i-1], fs[i])
			}
		}
	}
}

func TestCompactionMovesDataDownAndKeepsReads(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, smallOpts())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2000; i++ {
		if err := d.Put([]byte(fmt.Sprintf("key%05d", i)), []byte(fmt.Sprintf("val-%d-xxxxxxxxxxxx", i))); err != nil {
			t.Fatal(err)
		}
	}
	c := d.LevelCounts()
	if c[0] >= 3 || c[1]+c[2]+c[3] == 0 {
		t.Fatalf("expected compaction to have moved data below L0: %v", c)
	}
	checkLevelInvariants(t, d)
	verify := func(d *DB) {
		for i := 0; i < 2000; i++ {
			v, err := d.Get([]byte(fmt.Sprintf("key%05d", i)))
			if err != nil || string(v) != fmt.Sprintf("val-%d-xxxxxxxxxxxx", i) {
				t.Fatalf("key%05d: %q %v", i, v, err)
			}
		}
	}
	verify(d)
	d.Close()
	d, err = Open(dir, smallOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	checkLevelInvariants(t, d)
	verify(d)
	// Obsolete tables are gone from disk: files on disk == live tables.
	if n, live := len(glob(t, dir, "*.sst")), len(d.tables); n != live {
		t.Fatalf("%d sst files on disk, %d live", n, live)
	}
}

func TestCompactionDropsTombstonesAtBottom(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, smallOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for i := 0; i < 600; i++ {
		d.Put([]byte(fmt.Sprintf("k%04d", i)), []byte("some-value-some-value"))
	}
	for i := 0; i < 600; i++ {
		d.Delete([]byte(fmt.Sprintf("k%04d", i)))
	}
	// Push everything down with filler writes outside the deleted range.
	for i := 0; i < 600; i++ {
		d.Put([]byte(fmt.Sprintf("z%04d", i)), []byte("filler-filler-filler"))
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	var live int
	d.mu.RLock()
	for _, n := range d.allTables() {
		live += d.tables[n].Len()
	}
	d.mu.RUnlock()
	// 600 filler keys remain; tombstones and shadowed puts must be mostly gone.
	if live > 600+200 {
		t.Fatalf("tombstones/old versions not reclaimed: %d entries across tables", live)
	}
	for i := 0; i < 600; i++ {
		if _, err := d.Get([]byte(fmt.Sprintf("k%04d", i))); !errors.Is(err, ErrNotFound) {
			t.Fatalf("k%04d resurrected: %v", i, err)
		}
	}
}

// A tombstone in a shallow level must survive while a deeper level still
// holds the key it shadows.
func TestTombstoneKeptWhileDeeperDataExists(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, Options{NoSync: true, MemtableSize: 1 << 20, L0Trigger: 2, TargetFileSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	d.Put([]byte("a"), []byte("1"))
	d.Put([]byte("m"), []byte("1"))
	d.Flush()
	d.Put([]byte("b"), []byte("1"))
	d.Flush() // L0 -> L1 (a, b, m)
	d.mu.Lock()
	// Force a copy of the L1 data to L2 by compacting it down manually.
	if err := d.compactLocked(1, append([]uint64(nil), d.levels[1]...)); err != nil {
		t.Fatal(err)
	}
	d.mu.Unlock()
	if c := d.LevelCounts(); c[2] == 0 {
		t.Fatalf("setup failed: %v", c)
	}
	d.Delete([]byte("a"))
	d.Flush()
	d.Put([]byte("c"), []byte("1"))
	d.Flush() // L0 -> L1 with tombstone for a; L2 still has a
	if c := d.LevelCounts(); c[1] == 0 {
		t.Fatalf("expected L1 output: %v", c)
	}
	if _, err := d.Get([]byte("a")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a resurrected: %v", err)
	}
}

func TestCompactionModelWithReopens(t *testing.T) {
	dir := t.TempDir()
	rng := rand.New(rand.NewSource(7))
	model := map[string]string{}
	d, err := Open(dir, smallOpts())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 25000; i++ {
		k := fmt.Sprintf("key%03d", rng.Intn(500))
		switch rng.Intn(10) {
		case 0, 1, 2:
			d.Delete([]byte(k))
			delete(model, k)
		case 3:
			v, err := d.Get([]byte(k))
			want, ok := model[k]
			if ok != (err == nil) || (ok && string(v) != want) {
				t.Fatalf("op %d get %s: %q %v want %q %v", i, k, v, err, want, ok)
			}
		default:
			v := fmt.Sprintf("v%d-%s", i, bytes.Repeat([]byte("x"), rng.Intn(40)))
			d.Put([]byte(k), []byte(v))
			model[k] = v
		}
		if i%4000 == 3999 {
			d.Close()
			if d, err = Open(dir, smallOpts()); err != nil {
				t.Fatal(err)
			}
			checkLevelInvariants(t, d)
		}
	}
	defer d.Close()
	checkLevelInvariants(t, d)
	var keys []string
	for k := range model {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var got []string
	d.Scan(nil, nil, func(k, v []byte) bool {
		if model[string(k)] != string(v) {
			t.Fatalf("scan %s: %q want %q", k, v, model[string(k)])
		}
		got = append(got, string(k))
		return true
	})
	if fmt.Sprint(got) != fmt.Sprint(keys) {
		t.Fatalf("scan keys mismatch: %d vs %d", len(got), len(keys))
	}
	if c := d.LevelCounts(); c[1]+c[2] == 0 {
		t.Fatalf("test never compacted: %v", c)
	}
}

// Readers run concurrently with a writer that triggers flushes and compactions.
// Keys are written once in increasing generations, so a reader that saw
// generation g of a key must never later see an older one or a miss.
func TestReadsCorrectDuringCompaction(t *testing.T) {
	d, err := Open(t.TempDir(), smallOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	const nkeys = 100
	for i := 0; i < nkeys; i++ {
		d.Put([]byte(fmt.Sprintf("key%03d", i)), []byte("0000"))
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	errc := make(chan error, 4)
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(r)))
			seen := make([]string, nkeys)
			for {
				select {
				case <-stop:
					return
				default:
				}
				i := rng.Intn(nkeys)
				v, err := d.Get([]byte(fmt.Sprintf("key%03d", i)))
				if err != nil {
					errc <- fmt.Errorf("get key%03d: %v", i, err)
					return
				}
				if string(v) < seen[i] {
					errc <- fmt.Errorf("key%03d went backwards: %s after %s", i, v, seen[i])
					return
				}
				seen[i] = string(v)
			}
		}(r)
	}
	for g := 1; g <= 60; g++ {
		for i := 0; i < nkeys; i++ {
			if err := d.Put([]byte(fmt.Sprintf("key%03d", i)), []byte(fmt.Sprintf("%04d", g))); err != nil {
				t.Fatal(err)
			}
		}
	}
	close(stop)
	wg.Wait()
	select {
	case err := <-errc:
		t.Fatal(err)
	default:
	}
}

func TestOpenDeletesCompactionOrphans(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, smallOpts())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		d.Put([]byte(fmt.Sprintf("key%04d", i)), []byte("value-value-value"))
	}
	d.Close()
	// Simulate a crash before the compaction edit (orphan output) and after it
	// (an input that was never unlinked).
	orphan := sstPath(dir, 99999)
	if err := os.WriteFile(orphan, []byte("junk"), 0o644); err != nil {
		t.Fatal(err)
	}
	d, err = Open(dir, smallOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("orphan not deleted: %v", err)
	}
	for i := 0; i < 1000; i++ {
		if _, err := d.Get([]byte(fmt.Sprintf("key%04d", i))); err != nil {
			t.Fatal(err)
		}
	}
}
