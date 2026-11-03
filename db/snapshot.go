package db

import (
	"sort"
	"sync"
)

// Snapshot is a consistent, read-only view of the DB as of the moment it was
// taken. Later writes and deletes are invisible to it. While a snapshot is
// open, compaction keeps the versions it can see, so long-lived snapshots
// cost disk space; call Release when done. Snapshots do not survive Close.
type Snapshot struct {
	d    *DB
	seq  uint64
	once sync.Once
}

// NewSnapshot pins the current state of the database.
func (d *DB) NewSnapshot() (*Snapshot, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil, ErrClosed
	}
	d.snaps[d.seq]++
	return &Snapshot{d: d, seq: d.seq}, nil
}

// Seq returns the sequence number the snapshot reads at.
func (s *Snapshot) Seq() uint64 { return s.seq }

// Release lets compaction reclaim versions only this snapshot needed. It is
// safe to call more than once.
func (s *Snapshot) Release() {
	s.once.Do(func() {
		d := s.d
		d.mu.Lock()
		defer d.mu.Unlock()
		if d.snaps[s.seq]--; d.snaps[s.seq] <= 0 {
			delete(d.snaps, s.seq)
		}
	})
}

// Get reads key as of the snapshot.
func (s *Snapshot) Get(key []byte) ([]byte, error) {
	d := s.d
	d.nGets.Add(1)
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return nil, ErrClosed
	}
	return d.getAtLocked(key, s.seq)
}

// Scan is DB.Scan as of the snapshot.
func (s *Snapshot) Scan(start, end []byte, fn func(key, value []byte) bool) error {
	d := s.d
	d.nScans.Add(1)
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return ErrClosed
	}
	return d.scanAtLocked(start, end, s.seq, fn)
}

// snapshotSeqsLocked returns the live snapshot seqnums, ascending.
func (d *DB) snapshotSeqsLocked() []uint64 {
	out := make([]uint64, 0, len(d.snaps))
	for s := range d.snaps {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
