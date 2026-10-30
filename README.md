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
- **Corruption = end of log**: a bad record ends replay rather than being skipped, so recovery never yields a non-prefix of history.

## Roadmap
See [PLAN.md](PLAN.md). Day-by-day progress is in [WORKLOG.md](WORKLOG.md).

## License
MIT. See [LICENSE](LICENSE).
