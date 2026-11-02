package sstable

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
)

func openCached(t *testing.T, c *Cache, n int) (*Reader, [][]byte) {
	t.Helper()
	rng := rand.New(rand.NewSource(7))
	entries := buildEntries(rng, n)
	path := filepath.Join(t.TempDir(), "t.sst")
	write(t, path, entries)
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	r.SetCache(c)
	t.Cleanup(func() { r.Close() })
	var keys [][]byte
	for _, e := range entries {
		keys = append(keys, e.Key)
	}
	return r, keys
}

func TestCacheHitsAndSameResults(t *testing.T) {
	c := NewCache(1 << 20)
	r, keys := openCached(t, c, 2000)
	plain, _ := openCached(t, nil, 2000)
	for pass := 0; pass < 3; pass++ {
		for _, k := range keys[:300] {
			v1, f1, d1, err1 := r.Get(k, ^uint64(0))
			v2, f2, d2, err2 := plain.Get(k, ^uint64(0))
			if err1 != nil || err2 != nil || string(v1) != string(v2) || f1 != f2 || d1 != d2 {
				t.Fatalf("mismatch for %s", k)
			}
		}
	}
	s := c.Stats()
	if s.Hits == 0 || s.Misses == 0 || s.Hits < 2*s.Misses {
		t.Fatalf("expected mostly hits on repeated passes: %+v", s)
	}
}

func TestCacheEvictsAndBoundsSize(t *testing.T) {
	c := NewCache(4096)
	r, keys := openCached(t, c, 3000)
	for _, k := range keys {
		if _, _, _, err := r.Get(k, ^uint64(0)); err != nil {
			t.Fatal(err)
		}
	}
	s := c.Stats()
	if s.Bytes > s.Capacity || s.Evictions == 0 {
		t.Fatalf("cache not bounded: %+v", s)
	}
}

func TestCacheDroppedOnClose(t *testing.T) {
	c := NewCache(1 << 20)
	r, keys := openCached(t, c, 500)
	for _, k := range keys {
		r.Get(k, ^uint64(0))
	}
	if c.Stats().Entries == 0 {
		t.Fatal("expected cached blocks")
	}
	r.Close()
	if s := c.Stats(); s.Entries != 0 || s.Bytes != 0 {
		t.Fatalf("blocks left after close: %+v", s)
	}
}

func TestCacheLRUOrder(t *testing.T) {
	c := NewCache(100)
	mk := func() *block { return &block{data: make([]byte, 40)} }
	c.put(cacheKey{1, 0}, mk())
	c.put(cacheKey{1, 1}, mk())
	c.get(cacheKey{1, 0}) // 0 is now most recent
	c.put(cacheKey{1, 2}, mk())
	if _, ok := c.get(cacheKey{1, 1}); ok {
		t.Fatal("least recently used block should be evicted")
	}
	if _, ok := c.get(cacheKey{1, 0}); !ok {
		t.Fatal("recently used block was evicted")
	}
}

func TestCacheConcurrent(t *testing.T) {
	c := NewCache(8192)
	r, keys := openCached(t, c, 2000)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(g)))
			for i := 0; i < 2000; i++ {
				k := keys[rng.Intn(len(keys))]
				if _, _, _, err := r.Get(k, ^uint64(0)); err != nil {
					t.Error(fmt.Sprint(err))
					return
				}
			}
		}(g)
	}
	wg.Wait()
}
