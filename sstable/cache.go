package sstable

import (
	"container/list"
	"sync"
	"sync/atomic"
)

// CacheStats reports block cache activity.
type CacheStats struct {
	Hits, Misses, Evictions uint64
	Bytes, Capacity         int64
	Entries                 int
}

type cacheKey struct {
	reader uint64
	block  int
}

type cacheEntry struct {
	key  cacheKey
	b    *block
	size int64
}

// Cache is an LRU cache of verified, parsed data blocks shared by any number
// of Readers. Blocks are immutable, so cached blocks are handed out without
// copying. It is safe for concurrent use.
type Cache struct {
	mu       sync.Mutex
	capacity int64
	used     int64
	ll       *list.List // front = most recently used
	items    map[cacheKey]*list.Element

	hits, misses, evictions atomic.Uint64
}

// NewCache returns a cache holding at most capacity bytes of block data.
func NewCache(capacity int64) *Cache {
	return &Cache{capacity: capacity, ll: list.New(), items: map[cacheKey]*list.Element{}}
}

func (c *Cache) get(k cacheKey) (*block, bool) {
	c.mu.Lock()
	el, ok := c.items[k]
	if ok {
		c.ll.MoveToFront(el)
	}
	c.mu.Unlock()
	if !ok {
		c.misses.Add(1)
		return nil, false
	}
	c.hits.Add(1)
	return el.Value.(*cacheEntry).b, true
}

func (c *Cache) put(k cacheKey, b *block) {
	size := int64(len(b.data) + len(b.restarts))
	if size > c.capacity {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[k]; ok {
		c.ll.MoveToFront(el)
		return
	}
	c.items[k] = c.ll.PushFront(&cacheEntry{key: k, b: b, size: size})
	c.used += size
	for c.used > c.capacity {
		c.removeLocked(c.ll.Back())
		c.evictions.Add(1)
	}
}

func (c *Cache) removeLocked(el *list.Element) {
	e := c.ll.Remove(el).(*cacheEntry)
	delete(c.items, e.key)
	c.used -= e.size
}

// dropReader discards every block belonging to a closed Reader.
func (c *Cache) dropReader(id uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, el := range c.items {
		if k.reader == id {
			c.removeLocked(el)
		}
	}
}

// Stats returns a snapshot of the cache counters.
func (c *Cache) Stats() CacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return CacheStats{
		Hits: c.hits.Load(), Misses: c.misses.Load(), Evictions: c.evictions.Load(),
		Bytes: c.used, Capacity: c.capacity, Entries: len(c.items),
	}
}
