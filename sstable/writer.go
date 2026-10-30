// Package sstable implements immutable sorted string tables.
//
// File layout:
//
//	data block 0 … data block N-1   prefix-compressed, each with its own CRC
//	index block                     one entry per data block: last user key →
//	                                (offset, length)
//	bloom filter                    over user keys, with CRC
//	footer (52 bytes)               index/bloom locations, entry count, magic,
//	                                CRC32C of the preceding footer bytes
//
// Entries are ordered by user key ascending, then sequence number descending.
// All versions of one user key live in a single data block, so a point lookup
// reads at most one block.
package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"

	"github.com/Striv11/emberkv/memtable"
)

const (
	footerSize     = 52
	magic          = 0x656d6265726b7631 // "emberkv1"
	targetBlock    = 4096
	bloomBitsPerKy = 10
)

// Writer builds an SSTable. Entries must be added in table order. The file
// is written under a temporary name and renamed into place by Finish, so a
// crash never leaves a partial table at the final path.
type Writer struct {
	path, tmp string
	f         *os.File
	off       uint64
	blk       blockBuilder
	index     blockBuilder
	keys      [][]byte // distinct user keys, for the bloom filter
	lastKey   []byte
	lastSeq   uint64
	haveLast  bool
	count     uint64
	err       error
}

// NewWriter creates a writer that will produce path.
func NewWriter(path string) (*Writer, error) {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &Writer{path: path, tmp: tmp, f: f}, nil
}

// Add appends one entry. Keys must be non-decreasing, and for equal keys the
// sequence numbers must be strictly decreasing.
func (w *Writer) Add(e memtable.Entry) error {
	if w.err != nil {
		return w.err
	}
	if w.haveLast && !before(w.lastKey, w.lastSeq, e.Key, e.Seq) {
		return w.fail(fmt.Errorf("sstable: entry (%q,%d) out of order after (%q,%d)",
			e.Key, e.Seq, w.lastKey, w.lastSeq))
	}
	newKey := !w.haveLast || !bytes.Equal(w.lastKey, e.Key)
	// Cut only at user-key boundaries so a key's versions share a block.
	if newKey && w.blk.size() >= targetBlock {
		if err := w.flushBlock(); err != nil {
			return w.fail(err)
		}
	}
	if newKey {
		w.keys = append(w.keys, append([]byte(nil), e.Key...))
	}
	w.blk.add(e.Key, e.Seq, byte(e.Kind), e.Value)
	w.lastKey = append(w.lastKey[:0], e.Key...)
	w.lastSeq, w.haveLast = e.Seq, true
	w.count++
	return nil
}

func (w *Writer) fail(err error) error {
	w.err = err
	return err
}

func (w *Writer) writeRaw(p []byte) (off, n uint64, err error) {
	off = w.off
	if _, err = w.f.Write(p); err != nil {
		return 0, 0, err
	}
	w.off += uint64(len(p))
	return off, uint64(len(p)), nil
}

func (w *Writer) flushBlock() error {
	if w.blk.empty() {
		return nil
	}
	last := append([]byte(nil), w.blk.lastKey...)
	off, n, err := w.writeRaw(w.blk.finish())
	if err != nil {
		return err
	}
	handle := binary.AppendUvarint(binary.AppendUvarint(nil, off), n)
	w.index.add(last, 0, 0, handle)
	w.blk.reset()
	return nil
}

// Finish writes the index, filter and footer, syncs, and renames the file
// into place. The Writer must not be used afterwards.
func (w *Writer) Finish() error {
	if w.err != nil {
		return w.err
	}
	if err := w.flushBlock(); err != nil {
		return w.fail(err)
	}
	bl := newBloom(len(w.keys), bloomBitsPerKy)
	for _, k := range w.keys {
		bl.add(k)
	}
	idxOff, idxLen, err := w.writeRaw(w.index.finish())
	if err != nil {
		return w.fail(err)
	}
	enc := bl.encode()
	enc = binary.LittleEndian.AppendUint32(enc, crc32.Checksum(enc, castagnoli))
	blOff, blLen, err := w.writeRaw(enc)
	if err != nil {
		return w.fail(err)
	}
	var ft []byte
	for _, v := range []uint64{idxOff, idxLen, blOff, blLen, w.count, magic} {
		ft = binary.LittleEndian.AppendUint64(ft, v)
	}
	ft = binary.LittleEndian.AppendUint32(ft, crc32.Checksum(ft, castagnoli))
	if _, _, err := w.writeRaw(ft); err != nil {
		return w.fail(err)
	}
	if err := w.f.Sync(); err != nil {
		return w.fail(err)
	}
	if err := w.f.Close(); err != nil {
		return w.fail(err)
	}
	w.f = nil
	if err := os.Rename(w.tmp, w.path); err != nil {
		return w.fail(err)
	}
	return syncDir(w.path)
}

// Abort discards a partially written table.
func (w *Writer) Abort() error {
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return errors.Join(err, os.Remove(w.tmp))
}
