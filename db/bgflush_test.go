package db

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Striv11/emberkv/memtable"
)

// Writers keep going while a frozen memtable is flushed; reads must see both
// the active and the frozen memtable.
func TestBackgroundFlushReadsSeeFrozenMemtable(t *testing.T) {
	d, err := Open(t.TempDir(), bgOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	// Freeze a memtable by hand and keep the flusher from finishing it.
	d.mu.Lock()
	d.mem.Add(1, memtable.KindPut, []byte("a"), []byte("old"))
	d.seq = 1
	d.imm, d.immLogNum, d.immSeq = d.mem, d.logNum, d.seq
	d.mem = memtable.New()
	d.mu.Unlock()

	if v, err := d.Get([]byte("a")); err != nil || string(v) != "old" {
		t.Fatalf("get from imm: %q %v", v, err)
	}
	if err := d.Put([]byte("b"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	var keys []string
	d.Scan(nil, nil, func(k, v []byte) bool { keys = append(keys, string(k)); return true })
	if strings.Join(keys, ",") != "a,b" {
		t.Fatalf("scan = %v", keys)
	}
	if d.Stats().MemtableBytes == 0 {
		t.Fatal("stats should count the frozen memtable")
	}
	d.mu.Lock()
	d.imm = nil // the hand-made imm has no flush behind it
	d.mu.Unlock()
}

func TestBackgroundFlushManualAndStall(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, bgOpts())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4000; i++ {
		if err := d.Put([]byte(fmt.Sprintf("k%05d", i)), []byte("vvvvvvvvvvvvvvvvvvvvvvvv")); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := d.WaitCompaction(); err != nil {
		t.Fatal(err)
	}
	if st := d.Stats(); st.Flushes < 2 || st.MemtableBytes != 0 {
		t.Fatalf("flushes=%d memtable=%d", st.Flushes, st.MemtableBytes)
	}
	d.Close()
	// Everything flushed: only the active (empty) WAL segment remains.
	logs, _ := filepath.Glob(filepath.Join(dir, "*.log"))
	if len(logs) != 1 {
		t.Fatalf("wal segments left: %v", logs)
	}
	d, err = Open(dir, bgOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for i := 0; i < 4000; i++ {
		if _, err := d.Get([]byte(fmt.Sprintf("k%05d", i))); err != nil {
			t.Fatalf("k%05d: %v", i, err)
		}
	}
}

// Close while flushes are in flight must lose nothing: the frozen memtable's
// WAL segment is replayed on the next Open.
func TestBackgroundFlushCloseMidFlush(t *testing.T) {
	for round := 0; round < 15; round++ {
		dir := t.TempDir()
		d, err := Open(dir, bgOpts())
		if err != nil {
			t.Fatal(err)
		}
		n := 500 + round*37
		for i := 0; i < n; i++ {
			if err := d.Put([]byte(fmt.Sprintf("k%05d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
				t.Fatal(err)
			}
		}
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
		d, err = Open(dir, bgOpts())
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < n; i++ {
			v, err := d.Get([]byte(fmt.Sprintf("k%05d", i)))
			if err != nil || string(v) != fmt.Sprintf("v%d", i) {
				t.Fatalf("round %d k%05d: %q %v", round, i, v, err)
			}
		}
		d.Close()
		ents, _ := os.ReadDir(dir)
		for _, e := range ents {
			if strings.HasSuffix(e.Name(), ".tmp") {
				t.Fatalf("leftover %s", e.Name())
			}
		}
	}
}

func TestBackgroundFlushModelConcurrent(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, bgOpts())
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(11))
	model := map[string]string{}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var dmu sync.RWMutex // guards d across reopens
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			rr := rand.New(rand.NewSource(int64(r)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				dmu.RLock()
				d.Get([]byte(fmt.Sprintf("k%03d", rr.Intn(300))))
				d.Scan(nil, nil, func(k, v []byte) bool { return rr.Intn(50) != 0 })
				dmu.RUnlock()
			}
		}(r)
	}
	for i := 0; i < 15000; i++ {
		k := fmt.Sprintf("k%03d", rng.Intn(300))
		if rng.Intn(4) == 0 {
			delete(model, k)
			if err := d.Delete([]byte(k)); err != nil {
				t.Fatal(err)
			}
		} else {
			v := fmt.Sprintf("v%d-padpadpadpad", i)
			model[k] = v
			if err := d.Put([]byte(k), []byte(v)); err != nil {
				t.Fatal(err)
			}
		}
		if i%4000 == 3999 {
			dmu.Lock()
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			d, err = Open(dir, bgOpts())
			dmu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	close(stop)
	wg.Wait()
	check := func() {
		got := map[string]string{}
		d.Scan(nil, nil, func(k, v []byte) bool { got[string(k)] = string(v); return true })
		if len(got) != len(model) {
			t.Fatalf("len %d != %d", len(got), len(model))
		}
		for k, v := range model {
			if got[k] != v {
				t.Fatalf("%s: %q != %q", k, got[k], v)
			}
		}
	}
	check()
	if err := d.WaitCompaction(); err != nil {
		t.Fatal(err)
	}
	check()
	d.Close()
}
