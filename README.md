# emberkv

> A crash-safe LSM-tree key-value store built from scratch in Go, with a fault-injection harness that proves it survives crashes at any byte.

![CI](https://github.com/Striv11/emberkv/actions/workflows/ci.yml/badge.svg)

## Problem
Embedded storage engines (LevelDB, RocksDB, Pebble) must turn random writes into sequential I/O and serve reads from many immutable files, without losing acknowledged data when the process or machine dies mid-write. The hard parts are recovery and compaction, not the API.

## Motivation
emberkv builds every layer by hand: write-ahead log, memtable, SSTables with block indexes and bloom filters, leveled compaction, and manifest-based recovery. Correctness is checked by a harness that simulates crashes at every write offset and compares against a model, rather than by happy-path tests.

## Architecture
```
            Put/Delete                      Get / Scan
                │                               │
                ▼                               ▼
        ┌──────────────┐  append   ┌───────────────────────┐
        │     WAL      │◀──────────│  memtable (skiplist)  │──┐
        └──────────────┘           └───────────────────────┘  │ merge
                                         │ flush (full)       │ iterators
                                         ▼                    │
                         ┌────────── L0 SSTables ───────────┐ │
                         │ data blocks │ index │ bloom │ footer│◀┘
                         └───────────────┬────────────────────┘
                                         │ leveled compaction
                                         ▼
                                   L1 … Ln SSTables
                MANIFEST: atomic log of {added, removed} files per version
```
Key invariants:
- A write is acknowledged only after its WAL record is durable (or after group commit).
- WAL records carry CRC32C checksums; recovery replays the longest valid prefix and truncates any torn tail.
- SSTables are immutable. The set of live files changes only by appending to the MANIFEST.

## Setup
Requires Go 1.25+.
```sh
git clone https://github.com/Striv11/emberkv && cd emberkv
bash check.sh        # gofmt, go vet, tests
```

## Demo
*Planned (Day 7):* `emberkv bench` and `emberkv crashtest`. The crash test repeatedly kills a writer process at random points, reopens the database, and verifies it against a model.

## Design decisions & trade-offs
- **Single-file WAL with per-record CRC**: simple, and recovery is a linear scan. Segment rotation arrives with memtable flushes.
- **Versioned memtable**: a skiplist ordered by (key asc, seqnum desc) keeps every version and tombstone, so a read at seqnum N is one seek, and later snapshot reads and compaction need no format change.
- **WAL record = op + seqnum**: the DB logs `kind|seq|key|value`; replay rebuilds the memtable and resumes sequence numbering after the highest replayed seqnum.
- **SSTable blocks cut at user-key boundaries**: every version of a key lives in one data block, so a point lookup is bloom check, binary search of the in-memory index, and one block read. Blocks are prefix-compressed with restart points every 16 entries, and each block, the bloom filter and the footer carry their own CRC32C.
- **Atomic table creation**: the writer streams to `*.tmp`, fsyncs, then renames, so a crash never exposes a partial SSTable.
- **Flush protocol with a single commit point**: create the next WAL segment, write the memtable to an L0 table, then append one MANIFEST edit (add table + advance `LogNum`). Before that edit recovery replays the old segment and deletes the orphan table; after it, older segments are obsolete and are deleted on open if a crash left them behind. The MANIFEST reuses the WAL framing, so an edit is all-or-nothing, and it is rewritten as a snapshot when it grows.
- **Synchronous flush (for now)**: flushing blocks writers and readers under the DB lock. It keeps the protocol easy to reason about; background flush comes with compaction.
- **Corruption = end of log**: a bad record ends replay rather than being skipped, so recovery never yields a non-prefix of history.

- **Scan = k-way merge + version collapse**: `DB.Scan(start, end, fn)` merges the memtable and every L0 table through a heap ordered by (key asc, seq desc), then a collapsing layer keeps only the newest version at or below the snapshot seqnum and hides tombstoned keys. The scan holds the read lock, so it sees one consistent snapshot; the callback must not re-enter the DB.

## Roadmap
See [PLAN.md](PLAN.md). Day-by-day progress is in [WORKLOG.md](WORKLOG.md).

## License
MIT. See [LICENSE](LICENSE).
