package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// ErrCorrupt is wrapped by every error caused by malformed or damaged files.
var ErrCorrupt = fmt.Errorf("sstable: corrupt file")

func errCorrupt(msg string) error { return fmt.Errorf("%w: %s", ErrCorrupt, msg) }

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

const restartInterval = 16

// Block layout:
//
//	entries...                 shared uvarint | unshared uvarint | seq uvarint |
//	                           kind byte | vlen uvarint | key suffix | value
//	restart offsets            uint32 LE each; entries at these offsets store
//	                           their full key (shared == 0)
//	number of restarts         uint32 LE
//	crc32c of all above        uint32 LE
//
// Entries are ordered by key ascending then seq descending.

type blockBuilder struct {
	buf      []byte
	restarts []uint32
	lastKey  []byte
	n        int
}

func (b *blockBuilder) empty() bool { return b.n == 0 }

// size is the encoded size if finished now.
func (b *blockBuilder) size() int { return len(b.buf) + 4*len(b.restarts) + 8 }

func (b *blockBuilder) add(key []byte, seq uint64, kind byte, value []byte) {
	shared := 0
	if b.n%restartInterval == 0 {
		b.restarts = append(b.restarts, uint32(len(b.buf)))
	} else {
		for shared < len(key) && shared < len(b.lastKey) && key[shared] == b.lastKey[shared] {
			shared++
		}
	}
	b.buf = binary.AppendUvarint(b.buf, uint64(shared))
	b.buf = binary.AppendUvarint(b.buf, uint64(len(key)-shared))
	b.buf = binary.AppendUvarint(b.buf, seq)
	b.buf = append(b.buf, kind)
	b.buf = binary.AppendUvarint(b.buf, uint64(len(value)))
	b.buf = append(b.buf, key[shared:]...)
	b.buf = append(b.buf, value...)
	b.lastKey = append(b.lastKey[:0], key...)
	b.n++
}

func (b *blockBuilder) finish() []byte {
	out := b.buf
	for _, r := range b.restarts {
		out = binary.LittleEndian.AppendUint32(out, r)
	}
	out = binary.LittleEndian.AppendUint32(out, uint32(len(b.restarts)))
	out = binary.LittleEndian.AppendUint32(out, crc32.Checksum(out, castagnoli))
	return out
}

func (b *blockBuilder) reset() {
	b.buf, b.restarts, b.lastKey, b.n = nil, nil, b.lastKey[:0], 0
}

// block is a verified, parsed block.
type block struct {
	data     []byte // entry region only
	restarts []byte // raw restart offsets
	nRestart int
}

func parseBlock(raw []byte) (*block, error) {
	if len(raw) < 8 {
		return nil, errCorrupt("block too short")
	}
	body, sum := raw[:len(raw)-4], binary.LittleEndian.Uint32(raw[len(raw)-4:])
	if crc32.Checksum(body, castagnoli) != sum {
		return nil, errCorrupt("block checksum mismatch")
	}
	n := int(binary.LittleEndian.Uint32(body[len(body)-4:]))
	if n < 0 || 4*n > len(body)-4 {
		return nil, errCorrupt("bad restart count")
	}
	entriesEnd := len(body) - 4 - 4*n
	return &block{data: body[:entriesEnd], restarts: body[entriesEnd : len(body)-4], nRestart: n}, nil
}

func (b *block) restart(i int) int {
	return int(binary.LittleEndian.Uint32(b.restarts[4*i:]))
}

// blockIter iterates the entries of one block. Key and value slices are valid
// until the next call to next or seek.
type blockIter struct {
	b     *block
	off   int // offset of the next entry to decode
	key   []byte
	seq   uint64
	kind  byte
	value []byte
	err   error
}

func newBlockIter(b *block) *blockIter { return &blockIter{b: b} }

// next decodes the following entry, returning false at the end or on error.
func (it *blockIter) next() bool {
	if it.err != nil || it.off >= len(it.b.data) {
		return false
	}
	d := it.b.data[it.off:]
	read := func() uint64 {
		v, n := binary.Uvarint(d)
		if n <= 0 {
			it.err = errCorrupt("bad varint in block")
			return 0
		}
		d = d[n:]
		return v
	}
	shared := read()
	unshared := read()
	seq := read()
	if it.err != nil || len(d) < 1 {
		it.fail()
		return false
	}
	kind := d[0]
	d = d[1:]
	vlen := read()
	if it.err != nil || shared > uint64(len(it.key)) || unshared > uint64(len(d)) ||
		vlen > uint64(len(d))-unshared {
		it.fail()
		return false
	}
	it.key = append(it.key[:shared], d[:unshared]...)
	it.value = d[unshared : unshared+vlen]
	it.seq, it.kind = seq, kind
	it.off = len(it.b.data) - len(d) + int(unshared+vlen)
	return true
}

func (it *blockIter) fail() {
	if it.err == nil {
		it.err = errCorrupt("malformed block entry")
	}
}

func before(k1 []byte, s1 uint64, k2 []byte, s2 uint64) bool {
	if c := bytes.Compare(k1, k2); c != 0 {
		return c < 0
	}
	return s1 > s2
}

// seek positions the iterator on the first entry ordered at or after
// (key, seq), and reports whether there is one.
func (it *blockIter) seek(key []byte, seq uint64) bool {
	// Binary search for the last restart whose entry is strictly before the
	// target; scanning starts there.
	lo, hi := 0, it.b.nRestart
	for lo < hi {
		mid := (lo + hi) / 2
		it.off, it.key = it.b.restart(mid), it.key[:0]
		if it.off >= len(it.b.data) || !it.next() {
			it.fail()
			return false
		}
		if before(it.key, it.seq, key, seq) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	start := 0
	if lo > 0 {
		start = it.b.restart(lo - 1)
	}
	it.off, it.key = start, it.key[:0]
	for it.next() {
		if !before(it.key, it.seq, key, seq) {
			return true
		}
	}
	return false
}
