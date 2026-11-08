package db

import (
	"bytes"
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

// compaction is a unit of work chosen under the lock: the tables to merge
// and whether tombstones may be dropped.
type compaction struct {
	level  int
	inputs []uint64 // from level
	all    []uint64 // inputs plus overlapping tables of level+1
	bottom bool
	srcs   []merge.Source
	snaps  []uint64 // live snapshot seqnums, ascending, captured at plan time
}

// pickLocked chooses the next compaction, or nil if every level is within
// its target. d.mu must be held (read or write).
func (d *DB) pickLocked() *compaction {
	if d.opts.L0Trigger < 0 {
		return nil
	}
	level := -1
	var inputs []uint64
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
	return d.planLocked(level, inputs)
}

// planLocked expands inputs (tables of level) into a full compaction.
func (d *DB) planLocked(level int, inputs []uint64) *compaction {
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
	c := &compaction{level: level, inputs: inputs, bottom: true}
	c.all = append(append([]uint64(nil), inputs...), lower...)
	for l := out + 1; l < numLevels && c.bottom; l++ {
		if len(d.overlapping(l, lo, hi)) > 0 {
			c.bottom = false
		}
	}
	c.snaps = d.snapshotSeqsLocked()
	for _, n := range c.all {
		c.srcs = append(c.srcs, d.tables[n].NewIterator())
	}
	return c
}

// maybeCompactLocked runs compactions inline until no level exceeds its
// target. d.mu must be held for writing.
func (d *DB) maybeCompactLocked() error {
	for {
		c := d.pickLocked()
		if c == nil {
			return nil
		}
		if err := d.runLocked(c); err != nil {
			return err
		}
	}
}

// compactLocked forces a compaction of the given tables of level.
func (d *DB) compactLocked(level int, inputs []uint64) error {
	return d.runLocked(d.planLocked(level, inputs))
}

func (d *DB) runLocked(c *compaction) error {
	outputs, err := d.buildCompaction(c, func() uint64 { n := d.next; d.next++; return n })
	if err != nil {
		return err
	}
	return d.installCompactionLocked(c, outputs)
}

// buildCompaction merges c's inputs into new tables at level+1. It touches
// only immutable tables and the file-number allocator, so it may run without
// d.mu. On error it removes any partial output. Only the newest version of
// each key is kept, plus versions that live snapshots can still see. A
// tombstone is dropped when no deeper level overlaps the compacted range.
func (d *DB) buildCompaction(c *compaction, alloc func() uint64) ([]manifest.FileMeta, error) {
	out := c.level + 1
	it := merge.New(c.srcs...)

	var outputs []manifest.FileMeta
	var w *sstable.Writer
	var cur manifest.FileMeta
	var curBytes int64
	cleanup := func() {
		if w != nil {
			w.Abort()
		}
		for _, m := range outputs {
			d.fs.Remove(sstPath(d.dir, m.Num))
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
		if st, err := d.fs.Stat(sstPath(d.dir, cur.Num)); err == nil {
			cur.Size = uint64(st.Size())
		}
		outputs = append(outputs, cur)
		return nil
	}

	// Versions of a key arrive newest first. Keep the newest, plus any older
	// version that is the newest one visible to some snapshot (a snapshot
	// seqnum sn with e.Seq <= sn < lastKept).
	var lastKey []byte
	var lastKept uint64
	haveLast := false
	for it.Next() {
		e := it.Entry()
		first := !haveLast || !bytes.Equal(e.Key, lastKey)
		if first {
			lastKey, haveLast = append(lastKey[:0], e.Key...), true
			lastKept = e.Seq
			// A bottom tombstone can go only if no snapshot predates it;
			// otherwise older versions must stay visible beneath it.
			if e.Kind == memtable.KindDelete && c.bottom && (len(c.snaps) == 0 || c.snaps[0] >= e.Seq) {
				continue
			}
		} else {
			if !snapshotNeeds(c.snaps, e.Seq, lastKept) {
				continue
			}
			lastKept = e.Seq
		}
		// Cut only at user-key boundaries: point reads probe one table per
		// level, so all retained versions of a key must share a file.
		if w != nil && first && curBytes >= d.opts.TargetFileSize {
			if err := finish(); err != nil {
				cleanup()
				return nil, err
			}
		}
		if w == nil {
			num := alloc()
			var err error
			if w, err = sstable.NewWriterFS(d.fs, sstPath(d.dir, num)); err != nil {
				cleanup()
				return nil, err
			}
			cur, curBytes = manifest.FileMeta{Num: num, Level: out, Smallest: append([]byte(nil), e.Key...)}, 0
		}
		cur.Largest = append(cur.Largest[:0], e.Key...)
		curBytes += int64(len(e.Key) + len(e.Value) + 8)
		if err := w.Add(e); err != nil {
			cleanup()
			return nil, err
		}
	}
	if err := it.Err(); err != nil {
		cleanup()
		return nil, err
	}
	if err := finish(); err != nil {
		cleanup()
		return nil, err
	}
	return outputs, nil
}

// snapshotNeeds reports whether some snapshot seqnum sn (snaps ascending)
// satisfies seq <= sn < newer, i.e. a snapshot would read this version.
func snapshotNeeds(snaps []uint64, seq, newer uint64) bool {
	i := sort.Search(len(snaps), func(i int) bool { return snaps[i] >= seq })
	return i < len(snaps) && snaps[i] < newer
}

// installCompactionLocked commits a built compaction. The version edit that
// removes the inputs and adds the outputs is the single commit point. Before
// it, outputs are orphans that Open deletes; after it, the inputs are orphans
// that Open deletes. On error the outputs are removed. d.mu must be held for
// writing.
func (d *DB) installCompactionLocked(c *compaction, outputs []manifest.FileMeta) error {
	level, out, all := c.level, c.level+1, c.all
	readers := make(map[uint64]*sstable.Reader, len(outputs))
	closeNew := func() {
		for _, r := range readers {
			r.Close()
		}
		for _, m := range outputs {
			d.fs.Remove(sstPath(d.dir, m.Num))
		}
	}
	for _, m := range outputs {
		r, err := d.openTable(m.Num)
		if err != nil {
			closeNew()
			return err
		}
		readers[m.Num] = r
	}
	if err := d.man.Apply(manifest.Edit{NextFile: d.next, Added: outputs, Removed: all}); err != nil {
		closeNew()
		return err
	}

	// Committed: swap in-memory state, then delete the obsolete tables.
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
		d.fs.Remove(sstPath(d.dir, n))
	}
	d.nCompactions.Add(1)
	return nil
}

// compactLoop is the background compactor. It builds merged tables without
// holding d.mu, so reads and writes proceed meanwhile, and takes the lock only
// to pick work and to install the result. Flushes only add L0 tables newer
// than any input, so they never conflict with a build in flight.
func (d *DB) compactLoop() {
	defer d.wg.Done()
	alloc := func() uint64 {
		d.mu.Lock()
		defer d.mu.Unlock()
		n := d.next
		d.next++
		return n
	}
	for {
		d.mu.Lock()
		var c *compaction
		if !d.closed && d.bgErr == nil {
			c = d.pickLocked()
		}
		d.compacting = c != nil
		if c == nil {
			d.cond.Broadcast()
			d.mu.Unlock()
			if _, ok := <-d.kick; !ok {
				return
			}
			continue
		}
		d.mu.Unlock()

		var outputs []manifest.FileMeta
		first := true
		err := d.retryBuild(func() error {
			if !first { // iterators are single-use: rebuild them
				d.mu.RLock()
				c.srcs = c.srcs[:0]
				for _, n := range c.all {
					c.srcs = append(c.srcs, d.tables[n].NewIterator())
				}
				d.mu.RUnlock()
			}
			first = false
			var err error
			outputs, err = d.buildCompaction(c, alloc)
			return err
		})

		d.mu.Lock()
		if err == nil && d.closed {
			for _, m := range outputs {
				d.fs.Remove(sstPath(d.dir, m.Num))
			}
		} else if err == nil {
			err = d.installCompactionLocked(c, outputs)
		}
		if err != nil && d.bgErr == nil {
			d.bgErr = err
		}
		d.compacting = false
		d.cond.Broadcast()
		d.mu.Unlock()
	}
}

// WaitCompaction blocks until background compaction is idle (nothing to do
// or failed) and returns the background error, if any. In synchronous mode it
// returns immediately.
func (d *DB) WaitCompaction() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.opts.Background {
		return nil
	}
	for !d.closed && d.bgErr == nil && (d.imm != nil || d.compacting || d.pickLocked() != nil) {
		d.cond.Wait()
	}
	return d.bgErr
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
