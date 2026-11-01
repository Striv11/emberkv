// Package db is the emberkv storage engine facade. Writes are appended to the
// write-ahead log and applied to the memtable. When the memtable is full it is
// flushed to an L0 SSTable, the MANIFEST records the new version atomically,
// and the WAL segments it covered are deleted. Open recovers from the
// MANIFEST plus any WAL segments the MANIFEST does not yet cover.
package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/Striv11/emberkv/manifest"
	"github.com/Striv11/emberkv/memtable"
	"github.com/Striv11/emberkv/merge"
	"github.com/Striv11/emberkv/sstable"
	"github.com/Striv11/emberkv/wal"
)

// ErrNotFound is returned by Get for absent or deleted keys.
var ErrNotFound = errors.New("db: not found")

// ErrClosed is returned by operations on a closed DB.
var ErrClosed = errors.New("db: closed")

var errMalformed = errors.New("db: malformed record")

const (
	manifestName       = "MANIFEST"
	defaultMemtableMax = 4 << 20
)

// Options configures a DB.
type Options struct {
	// NoSync skips the fsync after each write. Acknowledged writes survive a
	// process crash only if flushed by Sync or Close.
	NoSync bool
	// MemtableSize is the approximate memtable size in bytes that triggers a
	// flush to L0. Zero means 4 MiB.
	MemtableSize int64
	// L0Trigger is the number of L0 tables that triggers an L0→L1
	// compaction. Zero means 4; a negative value disables compaction.
	L0Trigger int
	// L1Size is the target total size in bytes of level 1; each deeper level
	// is 10x larger. Zero means 10 MiB.
	L1Size int64
	// TargetFileSize is the approximate size of compaction output tables.
	// Zero means 2 MiB.
	TargetFileSize int64
	// Background runs compaction in a goroutine so writers and readers are
	// not blocked while tables are merged. Writers stall only when L0 grows
	// to three times L0Trigger tables.
	Background bool
}

// DB is a durable key-value store. It is safe for concurrent use.
type DB struct {
	dir  string
	opts Options

	mu     sync.RWMutex // guards everything below; serializes writers and flushes
	man    *manifest.Manifest
	log    *wal.Log
	logNum uint64
	mem    *memtable.Table
	tables map[uint64]*sstable.Reader
	meta   map[uint64]manifest.FileMeta
	// levels[0] holds L0 file numbers newest first (ranges may overlap).
	// levels[i>0] hold non-overlapping files sorted by smallest key.
	levels [numLevels][]uint64
	seq    uint64
	next   uint64 // next file number
	closed bool

	// Background compaction state (guarded by mu).
	cond       *sync.Cond
	kick       chan struct{}
	wg         sync.WaitGroup
	compacting bool
	bgErr      error
}

func walPath(dir string, num uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%06d.log", num))
}

func sstPath(dir string, num uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%06d.sst", num))
}

// Open opens or creates a database in dir.
func Open(dir string, opts Options) (*DB, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if opts.MemtableSize <= 0 {
		opts.MemtableSize = defaultMemtableMax
	}
	if opts.L0Trigger == 0 {
		opts.L0Trigger = 4
	}
	if opts.L1Size <= 0 {
		opts.L1Size = 10 << 20
	}
	if opts.TargetFileSize <= 0 {
		opts.TargetFileSize = 2 << 20
	}
	man, err := manifest.Open(filepath.Join(dir, manifestName))
	if err != nil {
		return nil, err
	}
	d := &DB{dir: dir, opts: opts, man: man, mem: memtable.New(), tables: map[uint64]*sstable.Reader{}, meta: map[uint64]manifest.FileMeta{}}
	d.cond = sync.NewCond(&d.mu)
	if err := d.recover(); err != nil {
		d.abandon()
		return nil, err
	}
	if opts.Background && opts.L0Trigger > 0 {
		d.kick = make(chan struct{}, 1)
		d.wg.Add(1)
		go d.compactLoop()
		d.kickCompaction() // recovery may have left work behind
	} else {
		d.opts.Background = false
	}
	return d, nil
}

// abandon releases resources after a failed Open.
func (d *DB) abandon() {
	for _, r := range d.tables {
		r.Close()
	}
	if d.log != nil {
		d.log.Close()
	}
	d.man.Close()
}

func (d *DB) recover() error {
	st := d.man.State()
	d.seq, d.next, d.logNum = st.LastSeq, st.NextFile, st.LogNum
	if d.next == 0 {
		d.next = 1
	}

	// Open live tables; delete files the MANIFEST does not know about
	// (outputs of a flush that crashed before its version edit).
	for _, f := range st.Sorted() {
		r, err := sstable.Open(sstPath(d.dir, f.Num))
		if err != nil {
			return fmt.Errorf("db: open table %d: %w", f.Num, err)
		}
		d.tables[f.Num] = r
		d.meta[f.Num] = f
		if f.Level >= numLevels {
			return fmt.Errorf("db: table %d at invalid level %d", f.Num, f.Level)
		}
		d.levels[f.Level] = append(d.levels[f.Level], f.Num)
		if f.Num >= d.next {
			d.next = f.Num + 1
		}
	}
	for l := 1; l < numLevels; l++ {
		d.sortLevel(l)
	}
	ents, err := os.ReadDir(d.dir)
	if err != nil {
		return err
	}
	var segs []uint64
	for _, e := range ents {
		name := e.Name()
		switch {
		case strings.HasSuffix(name, ".tmp"):
			os.Remove(filepath.Join(d.dir, name))
		case strings.HasSuffix(name, ".sst"):
			if n, ok := parseNum(name, ".sst"); ok {
				if _, live := st.Files[n]; !live {
					os.Remove(filepath.Join(d.dir, name))
				}
			}
		case strings.HasSuffix(name, ".log"):
			if n, ok := parseNum(name, ".log"); ok {
				if n < d.logNum {
					os.Remove(filepath.Join(d.dir, name)) // covered by a flushed table
				} else {
					segs = append(segs, n)
				}
			}
		}
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i] < segs[j] })

	replay := func(p []byte) error {
		seq, kind, key, value, err := decode(p)
		if err != nil {
			return err
		}
		d.mem.Add(seq, kind, key, value)
		if seq > d.seq {
			d.seq = seq
		}
		return nil
	}
	wopts := wal.Options{SyncOnAppend: !d.opts.NoSync}
	// Replay every uncovered segment in order. The last one stays open for
	// appends; earlier ones (left by a crash mid-flush) are replayed and
	// closed, and are deleted by the next flush advancing LogNum.
	for i, n := range segs {
		if n >= d.next {
			d.next = n + 1
		}
		log, _, err := wal.Open(walPath(d.dir, n), wopts, replay)
		if err != nil {
			return err
		}
		if i == len(segs)-1 {
			d.log, d.logNum = log, n
		} else if err := log.Close(); err != nil {
			return err
		}
	}
	if d.log == nil {
		n := d.next
		d.next++
		log, _, err := wal.Open(walPath(d.dir, n), wopts, nil)
		if err != nil {
			return err
		}
		d.log, d.logNum = log, n
		return d.man.Apply(manifest.Edit{NextFile: d.next})
	}
	return nil
}

func parseNum(name, suffix string) (uint64, bool) {
	n, err := strconv.ParseUint(strings.TrimSuffix(name, suffix), 10, 64)
	return n, err == nil
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
	for d.opts.Background && !d.closed && d.bgErr == nil && len(d.levels[0]) >= 3*d.opts.L0Trigger {
		d.cond.Wait() // L0 stall: let the compactor catch up
	}
	if d.closed {
		return ErrClosed
	}
	if d.bgErr != nil {
		return d.bgErr
	}
	seq := d.seq + 1
	if err := d.log.Append(encode(seq, kind, key, value)); err != nil {
		return err
	}
	d.seq = seq
	d.mem.Add(seq, kind, key, value)
	if d.mem.Size() >= d.opts.MemtableSize {
		return d.flushLocked()
	}
	return nil
}

// Flush writes the current memtable to an L0 SSTable immediately.
func (d *DB) Flush() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrClosed
	}
	return d.flushLocked()
}

// flushLocked runs the flush protocol; d.mu must be held for writing.
//
//  1. Create the next WAL segment (empty, so a crash here is harmless).
//  2. Write the memtable to a new SSTable (atomic via tmp+rename).
//  3. Append one MANIFEST edit: add the table, set LogNum to the new segment.
//     This is the commit point. Before it, recovery replays the old segment
//     and deletes the orphan table; after it, the old segment is obsolete.
//  4. Swap in an empty memtable, close and delete the old segment.
//
// Writers and readers are blocked for the duration; background flushing is a
// later optimization.
func (d *DB) flushLocked() error {
	if d.mem.Len() == 0 {
		return nil
	}
	if err := d.log.Sync(); err != nil {
		return err
	}
	newLogNum, tableNum := d.next, d.next+1
	newLog, _, err := wal.Open(walPath(d.dir, newLogNum), wal.Options{SyncOnAppend: !d.opts.NoSync}, nil)
	if err != nil {
		return err
	}
	meta, err := d.writeTable(tableNum)
	if err != nil {
		newLog.Close()
		os.Remove(walPath(d.dir, newLogNum))
		return err
	}
	r, err := sstable.Open(sstPath(d.dir, tableNum))
	if err != nil {
		newLog.Close()
		return err
	}
	err = d.man.Apply(manifest.Edit{
		LogNum: newLogNum, NextFile: tableNum + 1, LastSeq: d.seq,
		Added: []manifest.FileMeta{meta},
	})
	if err != nil {
		// Version not committed: the old segment stays authoritative.
		r.Close()
		newLog.Close()
		os.Remove(sstPath(d.dir, tableNum))
		os.Remove(walPath(d.dir, newLogNum))
		return err
	}
	oldLog, oldNum := d.log, d.logNum
	d.log, d.logNum, d.next = newLog, newLogNum, tableNum+1
	d.tables[tableNum] = r
	d.meta[tableNum] = meta
	d.levels[0] = append([]uint64{tableNum}, d.levels[0]...)
	d.mem = memtable.New()
	oldLog.Close()
	os.Remove(walPath(d.dir, oldNum))
	if d.opts.Background {
		d.kickCompaction()
		return nil
	}
	return d.maybeCompactLocked()
}

// kickCompaction wakes the background compactor (non-blocking).
func (d *DB) kickCompaction() {
	select {
	case d.kick <- struct{}{}:
	default:
	}
}

func (d *DB) writeTable(num uint64) (manifest.FileMeta, error) {
	meta := manifest.FileMeta{Num: num}
	w, err := sstable.NewWriter(sstPath(d.dir, num))
	if err != nil {
		return meta, err
	}
	it := d.mem.NewIterator()
	for it.Next() {
		e := it.Entry()
		if meta.Smallest == nil {
			meta.Smallest = append([]byte{}, e.Key...)
		}
		meta.Largest = append(meta.Largest[:0], e.Key...)
		if err := w.Add(e); err != nil {
			w.Abort()
			return meta, err
		}
	}
	if err := w.Finish(); err != nil {
		return meta, err
	}
	if st, err := os.Stat(sstPath(d.dir, num)); err == nil {
		meta.Size = uint64(st.Size())
	}
	return meta, nil
}

// Get returns the value for key, or ErrNotFound.
func (d *DB) Get(key []byte) ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return nil, ErrClosed
	}
	v, found, deleted := d.mem.Get(key, d.seq)
	if found {
		if deleted {
			return nil, ErrNotFound
		}
		return v, nil
	}
	// Shallower levels hold newer versions, and within L0 newer tables come
	// first, so the first hit is the newest version.
	for l := 0; l < numLevels; l++ {
		for _, n := range d.candidates(l, key) {
			v, found, deleted, err := d.tables[n].Get(key, d.seq)
			if err != nil {
				return nil, err
			}
			if found {
				if deleted {
					return nil, ErrNotFound
				}
				return v, nil
			}
		}
	}
	return nil, ErrNotFound
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

// Close syncs and closes the database. The memtable is not flushed; its
// contents stay in the WAL and are replayed by the next Open.
func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrClosed
	}
	d.closed = true
	d.cond.Broadcast()
	if d.kick != nil {
		close(d.kick)
		d.mu.Unlock()
		d.wg.Wait() // the compactor sees closed and discards unfinished output
		d.mu.Lock()
	}
	err := d.log.Close()
	for _, r := range d.tables {
		if cerr := r.Close(); err == nil {
			err = cerr
		}
	}
	if cerr := d.man.Close(); err == nil {
		err = cerr
	}
	return err
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

// Scan calls fn for every live key in [start, end) in ascending order, at a
// consistent snapshot. A nil start means the first key; a nil end means no
// upper bound. fn returning false stops the scan. The key and value slices
// are only valid during the call. fn must not call back into the DB, since
// the scan holds the read lock.
func (d *DB) Scan(start, end []byte, fn func(key, value []byte) bool) error {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return ErrClosed
	}
	srcs := []merge.Source{d.mem.NewIteratorFrom(start)}
	for _, n := range d.allTables() {
		it := d.tables[n].NewIterator()
		if start != nil {
			it.Seek(start)
		}
		srcs = append(srcs, it)
	}
	u := merge.NewUser(merge.New(srcs...), d.seq, end)
	for u.Next() {
		if !fn(u.Key(), u.Value()) {
			return nil
		}
	}
	return u.Err()
}
