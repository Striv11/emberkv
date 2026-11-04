package db

import "github.com/Striv11/emberkv/sstable"

// LevelStats describes one level of the tree.
type LevelStats struct {
	Tables int
	Bytes  uint64
}

// Stats is a point-in-time snapshot of DB counters and gauges.
type Stats struct {
	Puts, Deletes, Gets, Scans uint64 // operations since Open
	Flushes, Compactions       uint64 // completed since Open
	MemtableBytes              int64
	LastSeq                    uint64
	Levels                     [numLevels]LevelStats
	Cache                      sstable.CacheStats
}

// Stats returns a consistent snapshot of the DB's metrics.
func (d *DB) Stats() Stats {
	d.mu.RLock()
	defer d.mu.RUnlock()
	s := Stats{
		Puts: d.nPuts.Load(), Deletes: d.nDeletes.Load(),
		Gets: d.nGets.Load(), Scans: d.nScans.Load(),
		Flushes: d.nFlushes.Load(), Compactions: d.nCompactions.Load(),
		MemtableBytes: int64(d.mem.Size()),
		LastSeq:       d.seq,
	}
	if d.imm != nil {
		s.MemtableBytes += int64(d.imm.Size())
	}
	for l := range s.Levels {
		s.Levels[l].Tables = len(d.levels[l])
		for _, n := range d.levels[l] {
			s.Levels[l].Bytes += d.meta[n].Size
		}
	}
	if d.cache != nil {
		s.Cache = d.cache.Stats()
	}
	return s
}
