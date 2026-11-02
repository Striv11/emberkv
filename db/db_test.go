package db

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func open(t *testing.T, dir string) *DB {
	t.Helper()
	d, err := Open(dir, Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestPutGetDelete(t *testing.T) {
	d := open(t, t.TempDir())
	defer d.Close()
	if _, err := d.Get([]byte("a")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	d.Put([]byte("a"), []byte("1"))
	d.Put([]byte("a"), []byte("2"))
	if v, err := d.Get([]byte("a")); err != nil || string(v) != "2" {
		t.Fatalf("got %q %v", v, err)
	}
	d.Delete([]byte("a"))
	if _, err := d.Get([]byte("a")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound after delete, got %v", err)
	}
	if err := d.Delete([]byte("never-existed")); err != nil {
		t.Fatal(err)
	}
	d.Put([]byte("e"), nil)
	if v, err := d.Get([]byte("e")); err != nil || len(v) != 0 {
		t.Fatalf("empty value: %q %v", v, err)
	}
	d.Put(nil, []byte("emptykey"))
	if v, err := d.Get(nil); err != nil || string(v) != "emptykey" {
		t.Fatalf("empty key: %q %v", v, err)
	}
}

func TestReopenRestoresState(t *testing.T) {
	dir := t.TempDir()
	d := open(t, dir)
	d.Put([]byte("a"), []byte("1"))
	d.Put([]byte("b"), []byte("2"))
	d.Delete([]byte("a"))
	d.Close()

	d = open(t, dir)
	defer d.Close()
	if _, err := d.Get([]byte("a")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted key resurrected: %v", err)
	}
	if v, _ := d.Get([]byte("b")); string(v) != "2" {
		t.Fatalf("b = %q", v)
	}
	d.Put([]byte("a"), []byte("new"))
	if v, _ := d.Get([]byte("a")); string(v) != "new" {
		t.Fatalf("a = %q", v)
	}
}

func TestRandomizedAgainstModelWithReopens(t *testing.T) {
	dir := t.TempDir()
	d := open(t, dir)
	model := map[string]string{}
	rng := rand.New(rand.NewSource(99))
	check := func() {
		for i := 0; i < 100; i++ {
			k := fmt.Sprintf("key%d", i)
			v, err := d.Get([]byte(k))
			want, ok := model[k]
			if ok {
				if err != nil || string(v) != want {
					t.Fatalf("%s: got %q %v want %q", k, v, err, want)
				}
			} else if !errors.Is(err, ErrNotFound) {
				t.Fatalf("%s: want not found, got %q %v", k, v, err)
			}
		}
	}
	for i := 0; i < 5000; i++ {
		k := fmt.Sprintf("key%d", rng.Intn(100))
		if rng.Intn(5) == 0 {
			d.Delete([]byte(k))
			delete(model, k)
		} else {
			v := fmt.Sprintf("val%d", i)
			d.Put([]byte(k), []byte(v))
			model[k] = v
		}
		if i%500 == 499 {
			check()
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			d = open(t, dir)
			check()
		}
	}
	d.Close()
}

func TestTornWALRecoversPrefix(t *testing.T) {
	dir := t.TempDir()
	d := open(t, dir)
	for i := 0; i < 10; i++ {
		d.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v"))
	}
	d.Close()
	p := onlyWAL(t, dir)
	st, _ := os.Stat(p)
	if err := os.Truncate(p, st.Size()-3); err != nil {
		t.Fatal(err)
	}
	d = open(t, dir)
	defer d.Close()
	for i := 0; i < 9; i++ {
		if _, err := d.Get([]byte(fmt.Sprintf("k%d", i))); err != nil {
			t.Fatalf("k%d lost: %v", i, err)
		}
	}
	if _, err := d.Get([]byte("k9")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("torn record visible: %v", err)
	}
	d.Put([]byte("k9"), []byte("again"))
	if v, _ := d.Get([]byte("k9")); string(v) != "again" {
		t.Fatalf("k9 = %q", v)
	}
}

func TestConcurrentWriters(t *testing.T) {
	dir := t.TempDir()
	d := open(t, dir)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				d.Put([]byte(fmt.Sprintf("g%d-%d", g, i)), []byte("x"))
			}
		}()
	}
	wg.Wait()
	d.Close()
	d = open(t, dir)
	defer d.Close()
	for g := 0; g < 8; g++ {
		for i := 0; i < 200; i++ {
			if _, err := d.Get([]byte(fmt.Sprintf("g%d-%d", g, i))); err != nil {
				t.Fatalf("g%d-%d: %v", g, i, err)
			}
		}
	}
}

func TestClosed(t *testing.T) {
	d := open(t, t.TempDir())
	d.Close()
	if err := d.Put([]byte("a"), nil); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := d.Get([]byte("a")); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := d.Close(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	for _, p := range [][]byte{nil, {7}, {1}, {1, 1}, {1, 1, 200}} {
		if _, _, _, _, err := decode(p); err == nil {
			t.Errorf("decode(%v) accepted", p)
		}
	}
}

func onlyWAL(t *testing.T, dir string) string {
	t.Helper()
	m, _ := filepath.Glob(filepath.Join(dir, "*.log"))
	if len(m) != 1 {
		t.Fatalf("want one WAL segment, got %v", m)
	}
	return m[0]
}

func TestBlockCacheServesRepeatedReads(t *testing.T) {
	d, err := Open(t.TempDir(), Options{NoSync: true, MemtableSize: 4 << 10, L0Trigger: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for i := 0; i < 500; i++ {
		if err := d.Put([]byte(fmt.Sprintf("k%04d", i)), []byte("value-value-value")); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	for pass := 0; pass < 3; pass++ {
		for i := 0; i < 500; i++ {
			v, err := d.Get([]byte(fmt.Sprintf("k%04d", i)))
			if err != nil || string(v) != "value-value-value" {
				t.Fatalf("get %d: %q %v", i, v, err)
			}
		}
	}
	if s := d.CacheStats(); s.Hits == 0 || s.Hits < s.Misses {
		t.Fatalf("cache ineffective: %+v", s)
	}
}
