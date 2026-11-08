# Plan

Seven-day arc. Every milestone has acceptance criteria that can be checked by a test or a command.

| Day | Milestone | Acceptance criteria | Status |
|-----|-----------|---------------------|--------|
| 1 | Write-ahead log | CRC32C-framed records. Replay returns an exact prefix for a truncation at *every* byte offset. Corruption stops replay and truncates the tail. Concurrent appends are safe. | [x] |
| 2 | Memtable + DB facade | Skiplist memtable with seqnums and tombstones. `DB.Put/Get/Delete` are durable through the WAL. Reopening restores state. A randomized test matches a map model. | [x] |
| 3 | SSTable format | Writer/reader with prefix-compressed data blocks, block index, bloom filter, and checksummed footer. Point lookups and iteration are tested. Corrupt files are rejected. | [x] |
| 4 | Flush + MANIFEST | Memtable flushes to L0 when full. The MANIFEST records versions atomically. WAL segments are rotated and deleted after flush. Reopen recovers from MANIFEST + WAL. | [x] |
| 5 | Merging iterators + Scan | K-way merge across memtable and SSTables with newest-wins and tombstone handling. `DB.Scan(start, end)` is model-checked. | [x] |
| 6 | Leveled compaction | Background L0→L1→Ln compaction with size targets. Tombstones are dropped at the bottom level. Reads are correct during compaction (property test). Background mode via `Options.Background`. | [x] |
| 7 | Crash harness, CLI, benchmarks | `emberkv crashtest` kills a child writer at random points across 100+ iterations with zero acknowledged-write loss. `emberkv bench` reports fillseq/readrandom. README demo is complete. | [x] |

## Stretch
- [x] Power-loss simulation: a fault-injecting filesystem layer that drops unsynced bytes
- [x] Refresh README benchmark numbers and add a power-loss line to the crashtest docs
- [x] Inject write/sync errors in MemFS; WAL fails permanently after an I/O error
- [x] Move *flush* to a background goroutine too (compaction is done)
- [x] Block cache (LRU) and its metrics
- [x] Snapshots / MVCC reads at a seqnum
- [x] Prometheus-style stats endpoint
- [x] Fault-inject SSTable reads: failed reads surface as errors, never as misses
- [x] Fault-inject WAL/MANIFEST reads at open; read errors must fail Open, not truncate the log
- [x] Retry transient background flush/compaction build errors instead of failing the DB
