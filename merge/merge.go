// Package merge provides a k-way merging iterator over sorted entry sources
// (memtables and SSTables) and a user-facing iterator that collapses versions:
// newest visible version wins and tombstones hide a key.
package merge

import (
	"bytes"
	"container/heap"

	"github.com/Striv11/emberkv/memtable"
)

// Source is a sorted stream of entries ordered by (key asc, seq desc).
// Both memtable and sstable iterators satisfy it.
type Source interface {
	Next() bool
	Entry() memtable.Entry
	Err() error
}

func before(a, b memtable.Entry) bool {
	if c := bytes.Compare(a.Key, b.Key); c != 0 {
		return c < 0
	}
	return a.Seq > b.Seq
}

type item struct {
	src Source
	ent memtable.Entry
}

type minHeap []*item

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return before(h[i].ent, h[j].ent) }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x any)        { *h = append(*h, x.(*item)) }
func (h *minHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// Iterator merges sources into one stream ordered by (key asc, seq desc).
// Entries are valid until the next call to Next.
type Iterator struct {
	h       minHeap
	cur     *item // source whose entry was last returned; advanced lazily
	started bool
	srcs    []Source
	err     error
}

// New returns a merging iterator over srcs. Sources must be positioned before
// their first wanted entry (seeked, if needed) and not yet advanced.
func New(srcs ...Source) *Iterator { return &Iterator{srcs: srcs} }

func (it *Iterator) push(s Source) {
	if s.Next() {
		heap.Push(&it.h, &item{src: s, ent: s.Entry()})
	} else if err := s.Err(); err != nil && it.err == nil {
		it.err = err
	}
}

// Next advances and reports whether an entry is available.
func (it *Iterator) Next() bool {
	if it.err != nil {
		return false
	}
	if !it.started {
		it.started = true
		for _, s := range it.srcs {
			it.push(s)
		}
	} else if it.cur != nil {
		// Advance the source we last yielded from (it sits at the heap root).
		c := it.cur
		it.cur = nil
		if c.src.Next() {
			c.ent = c.src.Entry()
			heap.Fix(&it.h, 0)
		} else {
			if err := c.src.Err(); err != nil {
				it.err = err
				return false
			}
			heap.Pop(&it.h)
		}
		if it.err != nil {
			return false
		}
	}
	if it.err != nil || len(it.h) == 0 {
		return false
	}
	it.cur = it.h[0]
	return true
}

// Entry returns the current entry; valid until the next call to Next.
func (it *Iterator) Entry() memtable.Entry { return it.cur.ent }

// Err returns the first error encountered by any source.
func (it *Iterator) Err() error { return it.err }

// User yields, for each user key in [start, end), the newest version with
// seq <= snapshot, skipping keys whose newest visible version is a tombstone.
// A nil start means the beginning; a nil end means no upper bound.
type User struct {
	in       *Iterator
	snapshot uint64
	end      []byte
	key, val []byte
	skip     []byte // last user key resolved (older versions are shadowed)
	haveSkip bool
	done     bool
}

// NewUser wraps a merging iterator. The sources must already be seeked to start.
func NewUser(in *Iterator, snapshot uint64, end []byte) *User {
	return &User{in: in, snapshot: snapshot, end: end}
}

// Next advances to the next live key.
func (u *User) Next() bool {
	if u.done {
		return false
	}
	for u.in.Next() {
		e := u.in.Entry()
		if u.end != nil && bytes.Compare(e.Key, u.end) >= 0 {
			break
		}
		if e.Seq > u.snapshot || (u.haveSkip && bytes.Equal(e.Key, u.skip)) {
			continue
		}
		// First visible (newest) version of this key.
		u.skip, u.haveSkip = append(u.skip[:0], e.Key...), true
		if e.Kind == memtable.KindDelete {
			continue
		}
		u.key = append(u.key[:0], e.Key...)
		u.val = append(u.val[:0], e.Value...)
		return true
	}
	u.done = true
	return false
}

// Key returns the current key; valid until the next call to Next.
func (u *User) Key() []byte { return u.key }

// Value returns the current value; valid until the next call to Next.
func (u *User) Value() []byte { return u.val }

// Err returns the first error encountered.
func (u *User) Err() error { return u.in.Err() }
