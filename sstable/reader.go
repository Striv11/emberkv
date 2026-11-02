package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"

	"github.com/Striv11/emberkv/memtable"
)

func syncDir(path string) error {
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

type indexEntry struct {
	lastKey     []byte
	off, length uint64
}

// Reader serves lookups and iteration over one SSTable. It is safe for
// concurrent use.
type Reader struct {
	f     *os.File
	index []indexEntry
	bloom *bloom
	count uint64

	id    uint64 // unique per Reader; part of the block cache key
	cache *Cache
}

var nextReaderID atomic.Uint64

// SetCache makes the Reader serve data blocks through c. Call it before the
// Reader is shared between goroutines. A nil cache disables caching.
func (r *Reader) SetCache(c *Cache) { r.cache = c }

// Open opens and validates a table: footer, index and bloom filter are
// checksummed and parsed eagerly; data blocks are verified when read.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r, err := newReader(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return r, nil
}

func newReader(f *os.File) (*Reader, error) {
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := uint64(st.Size())
	if size < footerSize {
		return nil, errCorrupt("file too small")
	}
	ft := make([]byte, footerSize)
	if _, err := f.ReadAt(ft, int64(size-footerSize)); err != nil {
		return nil, err
	}
	if crc32.Checksum(ft[:footerSize-4], castagnoli) != binary.LittleEndian.Uint32(ft[footerSize-4:]) {
		return nil, errCorrupt("footer checksum mismatch")
	}
	var v [6]uint64
	for i := range v {
		v[i] = binary.LittleEndian.Uint64(ft[8*i:])
	}
	idxOff, idxLen, blOff, blLen, count, mg := v[0], v[1], v[2], v[3], v[4], v[5]
	if mg != magic {
		return nil, errCorrupt("bad magic")
	}
	limit := size - footerSize
	if idxOff > limit || idxLen > limit-idxOff || blOff > limit || blLen > limit-blOff {
		return nil, errCorrupt("footer offsets out of range")
	}
	r := &Reader{f: f, count: count, id: nextReaderID.Add(1)}

	raw, err := r.readAt(idxOff, idxLen)
	if err != nil {
		return nil, err
	}
	ib, err := parseBlock(raw)
	if err != nil {
		return nil, err
	}
	iit := newBlockIter(ib)
	for iit.next() {
		it := iit
		off, n1 := binary.Uvarint(it.value)
		length, n2 := binary.Uvarint(it.value[max(n1, 0):])
		if n1 <= 0 || n2 <= 0 || off > idxOff || length > idxOff-off {
			return nil, errCorrupt("bad index handle")
		}
		r.index = append(r.index, indexEntry{append([]byte(nil), it.key...), off, length})
	}
	if iit.err != nil {
		return nil, iit.err
	}

	raw, err = r.readAt(blOff, blLen)
	if err != nil {
		return nil, err
	}
	if len(raw) < 4 || crc32.Checksum(raw[:len(raw)-4], castagnoli) != binary.LittleEndian.Uint32(raw[len(raw)-4:]) {
		return nil, errCorrupt("bloom checksum mismatch")
	}
	if r.bloom, err = decodeBloom(raw[:len(raw)-4]); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Reader) readAt(off, n uint64) ([]byte, error) {
	buf := make([]byte, n)
	if _, err := r.f.ReadAt(buf, int64(off)); err != nil {
		return nil, fmt.Errorf("sstable: read: %w", err)
	}
	return buf, nil
}

func (r *Reader) readBlock(i int) (*block, error) {
	key := cacheKey{r.id, i}
	if r.cache != nil {
		if b, ok := r.cache.get(key); ok {
			return b, nil
		}
	}
	raw, err := r.readAt(r.index[i].off, r.index[i].length)
	if err != nil {
		return nil, err
	}
	b, err := parseBlock(raw)
	if err == nil && r.cache != nil {
		r.cache.put(key, b)
	}
	return b, err
}

// Len returns the number of entries (versions and tombstones).
func (r *Reader) Len() int { return int(r.count) }

// Close releases the file.
func (r *Reader) Close() error {
	if r.cache != nil {
		r.cache.dropReader(r.id)
	}
	return r.f.Close()
}

// blockFor returns the index of the only block that can hold key, or -1.
func (r *Reader) blockFor(key []byte) int {
	i := sort.Search(len(r.index), func(i int) bool {
		return bytes.Compare(r.index[i].lastKey, key) >= 0
	})
	if i == len(r.index) {
		return -1
	}
	return i
}

// Get returns the newest version of key with sequence <= seq, with the same
// semantics as memtable.Table.Get.
func (r *Reader) Get(key []byte, seq uint64) (value []byte, found, deleted bool, err error) {
	if !r.bloom.mayContain(key) {
		return nil, false, false, nil
	}
	bi := r.blockFor(key)
	if bi < 0 {
		return nil, false, false, nil
	}
	b, err := r.readBlock(bi)
	if err != nil {
		return nil, false, false, err
	}
	it := newBlockIter(b)
	if !it.seek(key, seq) {
		return nil, false, false, it.err
	}
	if !bytes.Equal(it.key, key) {
		return nil, false, false, nil
	}
	if memtable.Kind(it.kind) == memtable.KindDelete {
		return nil, true, true, nil
	}
	return append([]byte(nil), it.value...), true, false, nil
}

// Iterator walks all entries in table order.
type Iterator struct {
	r    *Reader
	bi   int // index of the next block to load
	cur  *blockIter
	seek []byte // pending seek target for the first loaded block
	err  error
}

// NewIterator returns an iterator positioned before the first entry.
func (r *Reader) NewIterator() *Iterator { return &Iterator{r: r} }

// Seek repositions the iterator so the next call to Next yields the first
// entry whose user key is >= key.
func (it *Iterator) Seek(key []byte) {
	it.cur, it.err = nil, nil
	it.seek = append([]byte{}, key...)
	it.bi = sort.Search(len(it.r.index), func(i int) bool {
		return bytes.Compare(it.r.index[i].lastKey, key) >= 0
	})
}

// Next advances and reports whether an entry is available.
func (it *Iterator) Next() bool {
	for it.err == nil {
		if it.cur != nil && it.cur.next() {
			return true
		}
		if it.cur != nil && it.cur.err != nil {
			it.err = it.cur.err
			return false
		}
		if it.bi >= len(it.r.index) {
			return false
		}
		b, err := it.r.readBlock(it.bi)
		if err != nil {
			it.err = err
			return false
		}
		it.bi++
		it.cur = newBlockIter(b)
		if it.seek != nil {
			target := it.seek
			it.seek = nil
			if it.cur.seek(target, ^uint64(0)) {
				return true
			}
			if it.cur.err != nil {
				it.err = it.cur.err
			}
			return false
		}
	}
	return false
}

// Entry returns the current entry; valid until the next call to Next.
func (it *Iterator) Entry() memtable.Entry {
	return memtable.Entry{
		Key: it.cur.key, Value: it.cur.value,
		Seq: it.cur.seq, Kind: memtable.Kind(it.cur.kind),
	}
}

// Err returns the first error encountered, if any.
func (it *Iterator) Err() error { return it.err }
