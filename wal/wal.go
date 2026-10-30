// Package wal implements an append-only write-ahead log with per-record
// checksums and torn-write recovery.
//
// On-disk record layout (little endian):
//
//	+----------+----------+-----------------+
//	| crc32c 4 | length 4 | payload (length) |
//	+----------+----------+-----------------+
//
// The checksum covers the length field and the payload. On open, the log is
// scanned from the start; the first record that is truncated or fails its
// checksum marks the end of durable data, and everything after it is cut off.
// That models a crash in the middle of a write: a prefix of records survives,
// never a corrupted record.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

const headerSize = 8

// MaxRecordSize bounds a single record so a corrupt length field cannot
// trigger a huge allocation during recovery.
const MaxRecordSize = 32 << 20

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ErrClosed is returned by operations on a closed log.
var ErrClosed = errors.New("wal: closed")

// ErrTooLarge is returned when appending a record over MaxRecordSize.
var ErrTooLarge = errors.New("wal: record too large")

// Options controls durability behaviour.
type Options struct {
	// SyncOnAppend fsyncs after every Append. Without it, callers decide
	// when to call Sync (group commit).
	SyncOnAppend bool
}

// Log is a single-file write-ahead log. It is safe for concurrent use.
type Log struct {
	mu     sync.Mutex
	f      *os.File
	w      *bufio.Writer
	opts   Options
	size   int64
	closed bool
}

// RecoveryInfo describes what Open found on disk.
type RecoveryInfo struct {
	Records        int   // valid records replayed
	ValidBytes     int64 // length of the durable prefix
	TruncatedBytes int64 // bytes discarded from a torn or corrupt tail
}

// Open opens (or creates) the log at path. It calls replay once for every
// valid record, in order, then truncates any torn tail so new appends follow
// the last good record. The payload passed to replay must not be retained.
func Open(path string, opts Options, replay func(payload []byte) error) (*Log, RecoveryInfo, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, RecoveryInfo{}, err
	}
	info, err := scan(f, replay)
	if err != nil {
		f.Close()
		return nil, info, err
	}
	if info.TruncatedBytes > 0 {
		if err := f.Truncate(info.ValidBytes); err != nil {
			f.Close()
			return nil, info, fmt.Errorf("wal: truncate torn tail: %w", err)
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, info, err
		}
	}
	if _, err := f.Seek(info.ValidBytes, io.SeekStart); err != nil {
		f.Close()
		return nil, info, err
	}
	l := &Log{f: f, w: bufio.NewWriterSize(f, 64<<10), opts: opts, size: info.ValidBytes}
	return l, info, nil
}

func scan(f *os.File, replay func([]byte) error) (RecoveryInfo, error) {
	st, err := f.Stat()
	if err != nil {
		return RecoveryInfo{}, err
	}
	total := st.Size()
	r := bufio.NewReaderSize(io.NewSectionReader(f, 0, total), 64<<10)
	var info RecoveryInfo
	var hdr [headerSize]byte
	var buf []byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			break // clean EOF or torn header
		}
		sum := binary.LittleEndian.Uint32(hdr[0:4])
		n := binary.LittleEndian.Uint32(hdr[4:8])
		if n > MaxRecordSize || int64(n) > total-info.ValidBytes-headerSize {
			break
		}
		if cap(buf) < int(n) {
			buf = make([]byte, n)
		}
		buf = buf[:n]
		if _, err := io.ReadFull(r, buf); err != nil {
			break
		}
		if checksum(hdr[4:8], buf) != sum {
			break
		}
		if replay != nil {
			if err := replay(buf); err != nil {
				return info, fmt.Errorf("wal: replay record %d: %w", info.Records, err)
			}
		}
		info.Records++
		info.ValidBytes += headerSize + int64(n)
	}
	info.TruncatedBytes = total - info.ValidBytes
	return info, nil
}

func checksum(lenField, payload []byte) uint32 {
	c := crc32.Update(0, crcTable, lenField)
	return crc32.Update(c, crcTable, payload)
}

// Append writes one record. It is durable once Append returns with
// SyncOnAppend set, or after the next successful Sync otherwise.
func (l *Log) Append(payload []byte) error {
	if len(payload) > MaxRecordSize {
		return ErrTooLarge
	}
	var hdr [headerSize]byte
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[0:4], checksum(hdr[4:8], payload))

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if _, err := l.w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := l.w.Write(payload); err != nil {
		return err
	}
	l.size += headerSize + int64(len(payload))
	if l.opts.SyncOnAppend {
		return l.syncLocked()
	}
	return nil
}

// Sync flushes buffered records and fsyncs the file.
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	return l.syncLocked()
}

func (l *Log) syncLocked() error {
	if err := l.w.Flush(); err != nil {
		return err
	}
	return l.f.Sync()
}

// Size returns the logical size of the log in bytes, including buffered data.
func (l *Log) Size() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.size
}

// Close syncs and closes the log.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	l.closed = true
	err := l.syncLocked()
	if cerr := l.f.Close(); err == nil {
		err = cerr
	}
	return err
}
