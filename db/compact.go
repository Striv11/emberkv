package db

import (
	"bytes"
	"os"
	"sort"

	"github.com/Striv11/emberkv/manifest"
	"github.com/Striv11/emberkv/memtable"
	"github.com/Striv11/emberkv/merge"
	"github.com/Striv11/emberkv/sstable"
)

const (
	numLevels   = 7
	levelFactor = 10
)

// sortLevel orders a level (>0) by smallest key.
func (d *DB) sortLevel(l int) {
	fs := d.levels[l]
	sort.Slice(fs, func(i, j int) bool {
		return bytes.Compare(d.meta[fs[i]].Smallest, d.meta[fs[j]].Smallest) < 0
	})
}

// candidates returns the tables in level l that may contain key, in
// newest-first order. L0 may return several; deeper levels at most one.
func (d *DB) candidates(l int, key []byte) []uint64 {
	fs := d.levels[l]
	if l == 0 {
		out := make([]uint64, 0, len(fs))
		for _, n := range fs {
			m := d.meta[n]
			if bytes.Compare(key, m.Smallest) >= 0 && bytes.Compare(key, m.Largest) <= 0 {
				out = append(out, n)
			}
		}
		return out
	}
	i := sort.Search(len(fs), func(i int) bool { return bytes.Compare(d.meta[fs[i]].Largest, key) >= 0 })
	if i < len(fs) && bytes.Compare(d.meta[fs[i]].Smallest, key) <= 0 {
		return fs[i : i+1]
	}
	return nil
}

// allTables lists every live table, shallowest level first.
func (d *DB) allTables() []uint64 {
	var out []uint64
	for l := 0; l < numLevels; l++ {
		out = append(out, d.levels[l]...)
	}
	return out
}

func (d *DB) levelTarget(l int) int64 {
	t := d.opts.L1Size
	for i := 1; i < l; i++ {
		t *= levelFactor
	}
	return t
}

func (d *DB) levelSize(l int) int64 {
	var s int64
	for _, n := range d.levels[l] {
		s += int64(d.meta[n].Size)
	}
	return s
}

func (d *DB) overlapping(l int, lo, hi []byte) []uint64 {
	var out []uint64
	for _, n := range d.levels[l] {
		m := d.meta[n]
		if bytes.Compare(m.Largest, lo) >= 0 && bytes.Compare(m.Smallest, hi) <= 0 {
			out = append(out, n)
		}
	}
	return out
}

// maybeCompactLocked runs compactions until no level exceeds its target.
// d.mu must be held for writing.
func (d *DB) maybeCompactLocked() error {
	if d.opts.L0Trigger < 0 {
		return nil
	}
	for {
		var inputs []uint64
		level := -1
		if len(d.levels[0]) >= d.opts.L0Trigger {
			level, inputs = 0, append([]uint64(nil), d.levels[0]...)
		} else {
			for l := 1; l < numLevels-1; l++ {
				if d.levelSize(l) > d.levelTarget(l) {
					// Oldest table first: a simple policy that cycles through the level.
					oldest := d.levels[l][0]
					for _, n := range d.levels[l] {
						if n < oldest {
							oldest = n
						}
					}
					level, inputs = l, []uint64{oldest}
					break
				}
			}
		}
		if level < 0 {
			return nil
		}
		if err := d.compactLocked(level, inputs); err != nil {
			return err
		}
	}
}

// compactLocked merges inputs (from level) and every overlapping table of
// level+1 into new tables at level+1.
//
// The version edit that removes the inputs and adds the outputs is the single
// commit point. Before it, outputs are orphans that Open deletes; after it,
// the inputs are orphans that Open deletes. Only the newest version of each
// key is kept, since reads are always at the latest sequence number. A
// tombstone is dropped when no deeper level overlaps the compacted range.
func (d *DB) compactLocked(level int, inputs []uint64) error {
	out := level + 1
	lo, hi := d.meta[inputs[0]].Smallest, d.meta[inputs[0]].Largest
	widen := func(ns []uint64) {
		for _, n := range ns {
			m := d.meta[n]
			if bytes.Compare(m.Smallest, lo) < 0 {
				lo = m.Smallest
			}
			if bytes.Compare(m.Largest, hi) > 0 {
				hi = m.Largest
			}
		}
	}
	widen(inputs)
	lower := d.overlapping(out, lo, hi)
	widen(lower)
	all := append(append([]uint64(nil), inputs...), lower...)
	bottom := true
	for l := out + 1; l < numLevels && bottom; l++ {
		if len(d.overlapping(l, lo, hi)) > 0 {
			bottom = false
		}
	}

	var srcs []merge.Source
	for _, n := range all {
		srcs = append(srcs, d.tables[n].NewIterator())
	}
	it := merge.New(srcs...)

	var outputs []manifest.FileMeta
	var w *sstable.Writer
	var cur manifest.FileMeta
	var curBytes int64
	next := d.next
	cleanup := func() {
		if w != nil {
			w.Abort()
		}
		for _, m := range outputs {
			os.Remove(sstPath(d.dir, m.Num))
		}
	}
	finish := func() error {
		if w == nil {
			return nil
		}
		err := w.Finish()
		w = nil
		if err != nil {
			return err
		}
		if st, err := os.Stat(sstPath(d.dir, cur.Num)); err == nil {
			cur.Size = uint64(st.Size())
		}
		outputs = append(outputs, cur)
		return nil
	}

	var lastKey []byte
	haveLast := false
	for it.Next() {
		e := it.Entry()
		if haveLast && bytes.Equal(e.Key, lastKey) {
			continue // older version of a key already handled
		}
		lastKey, haveLast = append(lastKey[:0], e.Key...), true
		if e.Kind == memtable.KindDelete && bottom {
			continue
		}
		if w != nil && curBytes >= d.opts.TargetFileSize {
			if err := finish(); err != nil {
				cleanup()
				return err
			}
		}
		if w == nil {
			var err error
			if w, err = sstable.NewWriter(sstPath(d.dir, next)); err != nil {
				cleanup()
				return err
			}
			cur, curBytes = manifest.FileMeta{Num: next, Level: out, Smallest: append([]byte(nil), e.Key...)}, 0
			next++
		}
		cur.Largest = append(cur.Largest[:0], e.Key...)
		curBytes += int64(len(e.Key) + len(e.Value) + 8)
		if err := w.Add(e); err != nil {
			cleanup()
			return err
		}
	}
	if err := it.Err(); err != nil {
		cleanup()
		return err
	}
	if err := finish(); err != nil {
		cleanup()
		return err
	}

	readers := make(map[uint64]*sstable.Reader, len(outputs))
	closeNew := func() {
		for _, r := range readers {
			r.Close()
		}
		cleanup()
	}
	for _, m := range outputs {
		r, err := sstable.Open(sstPath(d.dir, m.Num))
		if err != nil {
			closeNew()
			return err
		}
		readers[m.Num] = r
	}
	if err := d.man.Apply(manifest.Edit{NextFile: next, Added: outputs, Removed: all}); err != nil {
		closeNew()
		return err
	}

	// Committed: swap in-memory state, then delete the obsolete tables.
	d.next = next
	removed := make(map[uint64]bool, len(all))
	for _, n := range all {
		removed[n] = true
	}
	for l := level; l <= out; l++ {
		var kept []uint64
		for _, n := range d.levels[l] {
			if !removed[n] {
				kept = append(kept, n)
			}
		}
		d.levels[l] = kept
	}
	for _, m := range outputs {
		d.tables[m.Num] = readers[m.Num]
		d.meta[m.Num] = m
		d.levels[out] = append(d.levels[out], m.Num)
	}
	d.sortLevel(out)
	for _, n := range all {
		d.tables[n].Close()
		delete(d.tables, n)
		delete(d.meta, n)
		os.Remove(sstPath(d.dir, n))
	}
	return nil
}

// LevelCounts returns the number of live tables per level (for tests and diagnostics).
func (d *DB) LevelCounts() []int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	c := make([]int, numLevels)
	for l := range c {
		c[l] = len(d.levels[l])
	}
	return c
}
