// Package memtable implements an in-memory sorted table backed by a skiplist.
//
// Entries are ordered by user key ascending, then sequence number descending,
// so the newest version of a key is encountered first. Deletes are stored as
// tombstones rather than removing entries, which lets later layers (SSTables,
// compaction) see that a key was deleted.
package memtable

import (
	"bytes"
	"math/rand"
	"sync"
)

// Kind distinguishes a value from a tombstone.
type Kind uint8

const (
	KindDelete Kind = 0
	KindPut    Kind = 1
)

const maxHeight = 16

type node struct {
	key   []byte
	value []byte
	seq   uint64
	kind  Kind
	next  []*node
}

// Table is a skiplist memtable. Reads may run concurrently; writes are
// serialized by an internal lock.
type Table struct {
	mu     sync.RWMutex
	head   *node
	height int
	rng    *rand.Rand
	size   int64
	count  int
}

// New returns an empty table.
func New() *Table {
	return &Table{
		head:   &node{next: make([]*node, maxHeight)},
		height: 1,
		rng:    rand.New(rand.NewSource(1)),
	}
}

// less reports whether (k1,s1) sorts before (k2,s2).
func less(k1 []byte, s1 uint64, k2 []byte, s2 uint64) bool {
	if c := bytes.Compare(k1, k2); c != 0 {
		return c < 0
	}
	return s1 > s2
}

func (t *Table) randomHeight() int {
	h := 1
	for h < maxHeight && t.rng.Intn(4) == 0 {
		h++
	}
	return h
}

// findGE returns the first node >= (key, seq) in table order, filling prev
// (if non-nil) with the predecessor at every level.
func (t *Table) findGE(key []byte, seq uint64, prev []*node) *node {
	x := t.head
	for lvl := t.height - 1; lvl >= 0; lvl-- {
		for n := x.next[lvl]; n != nil && less(n.key, n.seq, key, seq); n = x.next[lvl] {
			x = n
		}
		if prev != nil {
			prev[lvl] = x
		}
	}
	return x.next[0]
}

// Add inserts a version of key. Sequence numbers must be unique per key;
// the caller (the DB) hands out a monotonically increasing sequence.
// Key and value are copied.
func (t *Table) Add(seq uint64, kind Kind, key, value []byte) {
	n := &node{
		key:   append([]byte(nil), key...),
		value: append([]byte(nil), value...),
		seq:   seq,
		kind:  kind,
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	var prev [maxHeight]*node
	t.findGE(n.key, seq, prev[:])
	h := t.randomHeight()
	if h > t.height {
		for i := t.height; i < h; i++ {
			prev[i] = t.head
		}
		t.height = h
	}
	n.next = make([]*node, h)
	for i := 0; i < h; i++ {
		n.next[i] = prev[i].next[i]
		prev[i].next[i] = n
	}
	t.size += int64(len(key) + len(value) + 8 + 1 + 8*h)
	t.count++
}

// Get returns the newest version of key with sequence <= seq. found is false
// if the table has no such version; if found, deleted tells whether it is a
// tombstone (value is then nil).
func (t *Table) Get(key []byte, seq uint64) (value []byte, found, deleted bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	// Versions sort seq-descending, so the first node >= (key, seq) is the
	// newest visible version if its key matches.
	n := t.findGE(key, seq, nil)
	if n == nil || !bytes.Equal(n.key, key) {
		return nil, false, false
	}
	if n.kind == KindDelete {
		return nil, true, true
	}
	return append([]byte(nil), n.value...), true, false
}

// Size returns an approximate memory footprint in bytes.
func (t *Table) Size() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.size
}

// Len returns the number of stored versions (including tombstones).
func (t *Table) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.count
}

// Entry is one stored version, as yielded by an iterator.
type Entry struct {
	Key, Value []byte
	Seq        uint64
	Kind       Kind
}

// Iterator walks entries in table order. It holds no lock; use it only when
// no writers are active (e.g. on a frozen memtable being flushed).
type Iterator struct{ n *node }

// NewIterator returns an iterator positioned before the first entry.
func (t *Table) NewIterator() *Iterator {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return &Iterator{n: &node{next: []*node{t.head.next[0]}}}
}

// Next advances and reports whether an entry is available.
func (it *Iterator) Next() bool {
	if it.n == nil {
		return false
	}
	it.n = it.n.next[0]
	return it.n != nil
}

// Entry returns the current entry; valid only after Next returned true.
func (it *Iterator) Entry() Entry {
	return Entry{Key: it.n.key, Value: it.n.value, Seq: it.n.seq, Kind: it.n.kind}
}
