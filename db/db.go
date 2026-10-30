// Package db is the emberkv storage engine facade. Writes are appended to the
// write-ahead log and then applied to the memtable; reopening replays the log
// to rebuild the memtable.
package db

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/Striv11/emberkv/memtable"
	"github.com/Striv11/emberkv/wal"
)

// ErrNotFound is returned by Get for absent or deleted keys.
var ErrNotFound = errors.New("db: not found")

// ErrClosed is returned by operations on a closed DB.
var ErrClosed = errors.New("db: closed")

var errMalformed = errors.New("db: malformed record")

const walName = "wal.log"

// Options configures a DB.
type Options struct {
	// NoSync skips the fsync after each write. Acknowledged writes survive a
	// process crash only if flushed by Sync or Close.
	NoSync bool
}

// DB is a durable key-value store. It is safe for concurrent use.
type DB struct {
	mu     sync.RWMutex // guards seq, closed; serializes writers
	log    *wal.Log
	mem    *memtable.Table
	seq    uint64
	closed bool
}

// Open opens or creates a database in dir, recovering from the WAL.
func Open(dir string, opts Options) (*DB, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	d := &DB{mem: memtable.New()}
	log, _, err := wal.Open(filepath.Join(dir, walName), wal.Options{SyncOnAppend: !opts.NoSync},
		func(p []byte) error {
			seq, kind, key, value, err := decode(p)
			if err != nil {
				return err
			}
			d.mem.Add(seq, kind, key, value)
			if seq > d.seq {
				d.seq = seq
			}
			return nil
		})
	if err != nil {
		return nil, err
	}
	d.log = log
	return d, nil
}

// Put stores value under key.
func (d *DB) Put(key, value []byte) error { return d.write(memtable.KindPut, key, value) }

// Delete removes key. Deleting an absent key is not an error.
func (d *DB) Delete(key []byte) error { return d.write(memtable.KindDelete, key, nil) }

func (d *DB) write(kind memtable.Kind, key, value []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrClosed
	}
	seq := d.seq + 1
	if err := d.log.Append(encode(seq, kind, key, value)); err != nil {
		return err
	}
	d.seq = seq
	d.mem.Add(seq, kind, key, value)
	return nil
}

// Get returns the value for key, or ErrNotFound.
func (d *DB) Get(key []byte) ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return nil, ErrClosed
	}
	v, found, deleted := d.mem.Get(key, d.seq)
	if !found || deleted {
		return nil, ErrNotFound
	}
	return v, nil
}

// Sync forces buffered writes to disk (useful with Options.NoSync).
func (d *DB) Sync() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrClosed
	}
	return d.log.Sync()
}

// Close syncs and closes the database.
func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrClosed
	}
	d.closed = true
	return d.log.Close()
}

// Record layout: kind(1) | seq uvarint | keyLen uvarint | key | value.
func encode(seq uint64, kind memtable.Kind, key, value []byte) []byte {
	buf := make([]byte, 0, 1+2*binary.MaxVarintLen64+len(key)+len(value))
	buf = append(buf, byte(kind))
	buf = binary.AppendUvarint(buf, seq)
	buf = binary.AppendUvarint(buf, uint64(len(key)))
	buf = append(buf, key...)
	return append(buf, value...)
}

func decode(p []byte) (seq uint64, kind memtable.Kind, key, value []byte, err error) {
	if len(p) < 1 {
		return 0, 0, nil, nil, errMalformed
	}
	kind = memtable.Kind(p[0])
	if kind != memtable.KindPut && kind != memtable.KindDelete {
		return 0, 0, nil, nil, errMalformed
	}
	p = p[1:]
	seq, n := binary.Uvarint(p)
	if n <= 0 {
		return 0, 0, nil, nil, errMalformed
	}
	p = p[n:]
	klen, n := binary.Uvarint(p)
	if n <= 0 || klen > uint64(len(p)-n) {
		return 0, 0, nil, nil, errMalformed
	}
	p = p[n:]
	return seq, kind, p[:klen], p[klen:], nil
}
