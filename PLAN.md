# Plan

Seven-day arc. Every milestone has acceptance criteria that can be checked by a test or a command.

| Day | Milestone | Acceptance criteria | Status |
|-----|-----------|---------------------|--------|
| 1 | Write-ahead log | CRC32C-framed records. Replay returns an exact prefix for a truncation at *every* byte offset. Corruption stops replay and truncates the tail. Concurrent appends are safe. | [x] |
| 2 | Memtable + DB facade | Skiplist memtable with seqnums and tombstones. `DB.Put/Get/Delete` are durable through the WAL. Reopening restores state. A randomized test matches a map model. | [x] |
| 3 | SSTable format | Writer/reader with prefix-compressed data blocks, block index, bloom filter, and checksummed footer. Point lookups and iteration are tested. Corrupt files are rejected. | [x] |
| 4 | Flush + MANIFEST | Memtable flushes to L0 when full. The MANIFEST records versions atomically. WAL segments are rotated and deleted after flush. Reopen recovers from MANIFEST + WAL. | [x] |
| 5 | Merging iterators + Scan | K-way merge across memtable and SSTables with newest-wins and tombstone handling. `DB.Scan(start, end)` is model-checked. | [x] |
| 6 | Leveled compaction | Background L0→L1→Ln compaction with size targets. Tombstones are dropped at the bottom level. Reads are correct during compaction (property test). *Done except "background": compaction runs synchronously under the DB lock.* | [~] |
| 7 | Crash harness, CLI, benchmarks | `emberkv crashtest` kills a child writer at random points across 100+ iterations with zero acknowledged-write loss. `emberkv bench` reports fillseq/readrandom. README demo is complete. | [x] |

## Stretch
- Power-loss simulation: a fault-injecting filesystem layer that drops unsynced bytes
- Move flush and compaction to a background goroutine (finish Day 6)
- Block cache (LRU) and its metrics
- Snapshots / MVCC reads at a seqnum
- Prometheus-style stats endpoint
