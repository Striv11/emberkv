// Package manifest records which SSTables are live. It is an append-only log
// of version edits, framed by the WAL package (CRC32C, torn tail truncated),
// so a version change is atomic: an edit either replays completely or not at all.
package manifest

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/Striv11/emberkv/wal"
)

var errMalformed = errors.New("manifest: malformed edit")

// snapshotThreshold is the number of replayed edits after which Open rewrites
// the manifest as a single snapshot edit.
const snapshotThreshold = 256

// FileMeta describes one live SSTable.
type FileMeta struct {
	Num      uint64
	Level    int
	Size     uint64
	Smallest []byte
	Largest  []byte
}

// Edit is one atomic change to the version.
type Edit struct {
	LogNum   uint64 // WAL segments below this number are obsolete (0 = unchanged)
	NextFile uint64 // lowest unused file number (0 = unchanged)
	LastSeq  uint64 // highest sequence number covered by flushed tables (0 = unchanged)
	Added    []FileMeta
	Removed  []uint64
}

// State is the materialized version after replaying all edits.
type State struct {
	LogNum   uint64
	NextFile uint64
	LastSeq  uint64
	Files    map[uint64]FileMeta
}

func (s *State) apply(e Edit) {
	if e.LogNum > s.LogNum {
		s.LogNum = e.LogNum
	}
	if e.NextFile > s.NextFile {
		s.NextFile = e.NextFile
	}
	if e.LastSeq > s.LastSeq {
		s.LastSeq = e.LastSeq
	}
	for _, n := range e.Removed {
		delete(s.Files, n)
	}
	for _, f := range e.Added {
		s.Files[f.Num] = f
	}
}

// Sorted returns the live files ordered by level, then newest (highest number) first.
func (s *State) Sorted() []FileMeta {
	out := make([]FileMeta, 0, len(s.Files))
	for _, f := range s.Files {
		out = append(out, f)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Level != out[j].Level {
			return out[i].Level < out[j].Level
		}
		return out[i].Num > out[j].Num
	})
	return out
}

// Manifest is an open manifest log. It is safe for concurrent use.
type Manifest struct {
	mu    sync.Mutex
	log   *wal.Log
	state State
}

// Open opens or creates the manifest at path and replays it.
func Open(path string) (*Manifest, error) {
	st := State{Files: map[uint64]FileMeta{}}
	edits := 0
	log, _, err := wal.Open(path, wal.Options{SyncOnAppend: true}, func(p []byte) error {
		e, err := decode(p)
		if err != nil {
			return err
		}
		st.apply(e)
		edits++
		return nil
	})
	if err != nil {
		return nil, err
	}
	m := &Manifest{log: log, state: st}
	if edits > snapshotThreshold {
		if err := m.rewrite(path); err != nil {
			log.Close()
			return nil, err
		}
	}
	return m, nil
}

// rewrite replaces the manifest with one snapshot edit, atomically via rename.
func (m *Manifest) rewrite(path string) error {
	tmp := path + ".tmp"
	if err := os.Remove(tmp); err != nil && !os.IsNotExist(err) {
		return err
	}
	nl, _, err := wal.Open(tmp, wal.Options{SyncOnAppend: true}, nil)
	if err != nil {
		return err
	}
	snap := Edit{LogNum: m.state.LogNum, NextFile: m.state.NextFile, LastSeq: m.state.LastSeq, Added: m.state.Sorted()}
	if err := nl.Append(encode(snap)); err != nil {
		nl.Close()
		return err
	}
	if err := nl.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if err := syncDir(path); err != nil {
		return err
	}
	m.log.Close()
	m.log, _, err = wal.Open(path, wal.Options{SyncOnAppend: true}, nil)
	return err
}

func syncDir(path string) error {
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// State returns a copy of the current version.
func (m *Manifest) State() State {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := m.state
	cp.Files = make(map[uint64]FileMeta, len(m.state.Files))
	for k, v := range m.state.Files {
		cp.Files[k] = v
	}
	return cp
}

// Apply durably appends e, then applies it to the in-memory state.
func (m *Manifest) Apply(e Edit) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.log.Append(encode(e)); err != nil {
		return err
	}
	m.state.apply(e)
	return nil
}

// Close closes the manifest.
func (m *Manifest) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.log.Close()
}

func encode(e Edit) []byte {
	var b []byte
	b = binary.AppendUvarint(b, e.LogNum)
	b = binary.AppendUvarint(b, e.NextFile)
	b = binary.AppendUvarint(b, e.LastSeq)
	b = binary.AppendUvarint(b, uint64(len(e.Added)))
	for _, f := range e.Added {
		b = binary.AppendUvarint(b, f.Num)
		b = binary.AppendUvarint(b, uint64(f.Level))
		b = binary.AppendUvarint(b, f.Size)
		b = binary.AppendUvarint(b, uint64(len(f.Smallest)))
		b = append(b, f.Smallest...)
		b = binary.AppendUvarint(b, uint64(len(f.Largest)))
		b = append(b, f.Largest...)
	}
	b = binary.AppendUvarint(b, uint64(len(e.Removed)))
	for _, n := range e.Removed {
		b = binary.AppendUvarint(b, n)
	}
	return b
}

type reader struct {
	p   []byte
	err error
}

func (r *reader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.p)
	if n <= 0 {
		r.err = errMalformed
		return 0
	}
	r.p = r.p[n:]
	return v
}

func (r *reader) bytes() []byte {
	n := r.uvarint()
	if r.err != nil {
		return nil
	}
	if n > uint64(len(r.p)) {
		r.err = errMalformed
		return nil
	}
	out := append([]byte(nil), r.p[:n]...)
	r.p = r.p[n:]
	return out
}

func decode(p []byte) (Edit, error) {
	r := &reader{p: p}
	var e Edit
	e.LogNum, e.NextFile, e.LastSeq = r.uvarint(), r.uvarint(), r.uvarint()
	n := r.uvarint()
	if r.err == nil && n > uint64(len(r.p)) { // each entry needs at least one byte
		return e, errMalformed
	}
	for i := uint64(0); i < n && r.err == nil; i++ {
		var f FileMeta
		f.Num, f.Level, f.Size = r.uvarint(), int(r.uvarint()), r.uvarint()
		f.Smallest, f.Largest = r.bytes(), r.bytes()
		e.Added = append(e.Added, f)
	}
	n = r.uvarint()
	if r.err == nil && n > uint64(len(r.p)) {
		return e, errMalformed
	}
	for i := uint64(0); i < n && r.err == nil; i++ {
		e.Removed = append(e.Removed, r.uvarint())
	}
	if r.err == nil && len(r.p) != 0 {
		r.err = errMalformed
	}
	if r.err != nil {
		return Edit{}, fmt.Errorf("%w", r.err)
	}
	return e, nil
}
