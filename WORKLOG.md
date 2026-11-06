# Work log

Newest entries at the bottom. Each entry records what actually changed, what was learned, and what comes next.

## 2026-10-30: Day 1, write-ahead log
**Changed:** Added `wal` package: an append-only log with CRC32C (Castagnoli) over length+payload, buffered writes, optional fsync per append, explicit `Sync` for group commit, and recovery that replays the longest valid prefix then truncates the torn tail.
**Tests:** a truncation at every byte offset of a 20-record log recovers an exact prefix and accepts new appends; a bit flip mid-log stops replay at the previous record; random garbage files never panic; 8 concurrent writers lose nothing.
**Learned:** a length field must be bounds-checked against the remaining file size before allocating, or a corrupted header can request gigabytes.
**Next:** skiplist memtable with sequence numbers and tombstones, and a `DB` facade over WAL + memtable.

## 2026-10-30: Day 2, memtable + DB facade
**Changed:** Added `memtable` (skiplist ordered by key asc / seqnum desc, tombstones, snapshot-style `Get(key, seq)`, ordered iterator for the future flush) and `db` (`Open/Put/Get/Delete/Sync/Close`). Writes append a `kind|seq|key|value` record to the WAL, then update the memtable; `Open` replays the WAL and resumes sequence numbers.
**Tests:** memtable version visibility per seqnum, iterator order vs sorted model, 20k random ops vs map; DB randomized model check with periodic close/reopen, torn WAL tail recovers a prefix and seqnums continue, 8 concurrent writers survive reopen, closed-DB errors, malformed record decoding. All pass under `-race`.
**Learned:** ordering seq descending makes "newest version visible at seq N" a single `findGE(key, N)`, with no scan over versions.
**Next:** SSTable format (prefix-compressed blocks, index, bloom filter, checksummed footer).

## 2026-10-30: Day 3, SSTable format
**Changed:** Added `sstable` package. `Writer` takes `memtable.Entry` values in table order, emits prefix-compressed data blocks (restart every 16 entries), an index block, a bloom filter (10 bits/key, double hashing) and a 52-byte checksummed footer; it writes to `.tmp`, fsyncs and renames. `Reader` validates footer/index/bloom at `Open`, verifies each data block's CRC on read, and offers `Get(key, seq)` (same semantics as the memtable) plus an `Iterator` with `Seek`.
**Tests:** full round trip over 3000 keys with multiple versions and tombstones; 20k random `Get`s compared against a memtable holding the same data; seek from many targets; a hot key with 50 large versions stays in one block; empty table; out-of-order `Add` rejected; abort leaves no files; bloom false-positive rate under 3%; flipping bytes across the whole file and truncating at many lengths is always detected as `ErrCorrupt`. Passes under `-race`. Point lookup benchmark is about 1.9us (each Get reads and CRCs one block, no cache yet).
**Learned:** cutting blocks only at user-key boundaries keeps lookups to a single block and makes the index key (last user key) unambiguous, at the cost of slightly oversized blocks for hot keys.
**Next:** flush the memtable to L0, add the MANIFEST, rotate and delete WAL segments, and recover from MANIFEST + WAL.

## 2026-10-30: Day 4, flush + MANIFEST
**Changed:** Added `manifest` (append-only log of version edits reusing WAL framing; replays to a `State` of live files, `LogNum`, `NextFile`, `LastSeq`; rewrites itself as one snapshot edit past 256 edits). `db` now numbers files, rotates WAL segments (`NNNNNN.log`), flushes the memtable to an L0 SSTable when it exceeds `Options.MemtableSize` (or on `Flush()`), commits with a single MANIFEST edit, then deletes the covered WAL. `Get` checks the memtable then L0 tables newest-first. `Open` loads tables from the MANIFEST, deletes orphan `.sst`/`.tmp` files and WAL segments below `LogNum`, and replays the rest.
**Tests:** manifest replay, torn-tail atomicity of an edit, snapshot rewrite, garbage decode; db flush/rotation/reopen, tombstones shadowing older tables, crash before the MANIFEST commit (orphan table + empty WAL), crash after it (resurrected stale WAL must not be replayed), corrupt table fails open, 30k-op randomized model check with flushes and reopens. All pass under `-race`.
**Learned:** persisting `LastSeq` in the edit matters: once the WAL is deleted, the memtable is empty and sequence numbers would otherwise restart, letting old table data shadow new writes.
**Next:** merging iterators across memtable and SSTables and `DB.Scan(start, end)`, model-checked. Flush is still synchronous under the DB lock; revisit with compaction.

## 2026-10-31: Day 5, merging iterators + Scan
**Changed:** Added `merge` package: a heap-based k-way `Iterator` over any sorted `Source` (memtable or SSTable iterators) ordered by (key asc, seq desc), and a `User` iterator that collapses versions (newest at or below the snapshot wins, tombstones hide the key, exclusive `end` bound). Added `memtable.NewIteratorFrom` for seeking. `DB.Scan(start, end, fn)` merges memtable plus all L0 tables under the read lock.
**Tests:** merge order with empty sources, 1200-entry random merge vs sort, snapshot/tombstone/end-bound semantics, error propagation from a source; memtable seek; DB scan basics (overwrite, delete, early stop, closed) and a 15k-op randomized model check with flushes, reopens and random bounds.
**Learned:** the version-collapse state (last resolved key) has to live on the iterator, not in `Next`, or older versions of an already-returned key look like fresh keys on the next call.
**Next:** leveled compaction (L0 to L1 to Ln) with size targets, tombstone dropping at the bottom level, and moving flush/compaction off the DB lock.

## 2026-10-31: Day 6, leveled compaction
**Changed:** `db` now keeps tables in levels (L0 overlapping, newest first; L1-L6 sorted and non-overlapping, with per-file key ranges from the MANIFEST). After each flush, `maybeCompactLocked` merges all L0 tables plus overlapping L1 tables once `L0Trigger` is reached, and pushes a table down from any deeper level that exceeds its size target (x10 per level). Output is split at `TargetFileSize`, keeps only the newest version per key, and drops tombstones when no deeper level overlaps the range. One MANIFEST edit adds outputs and removes inputs. `Get` consults one table per level; `Scan` merges all levels.
**Tests:** data moves below L0 and survives reopen, with no stale `.sst` files left; tombstones are reclaimed at the bottom but kept while deeper data exists; 25k-op model check with reopens and non-overlap invariant; 4 concurrent readers during a writer that triggers compactions (monotonic values, under `-race`); orphan files are deleted on Open.
**Learned:** the tombstone-drop check has to use the key range widened by the overlapping next-level inputs, not just the picked inputs, or a shadowed key in a deeper level can resurrect.
**Not done:** compaction is synchronous under the DB lock, so Day 6 is marked partial (`[~]`). Next: a background flush/compaction goroutine, then Day 7 (crash harness, CLI, benchmarks).

## 2026-11-01: Day 7, crash harness, CLI, benchmarks
**Changed:** Added `cmd/emberkv` with `crashtest` and `bench`. `crashtest` spawns itself as a writer child that applies a deterministic put/delete stream and prints `ack i` after each durable op; the parent SIGKILLs it at a random time, reopens the DB and compares a full scan to the model after N or N+1 ops (N = acked), then resumes from whichever matched. Tiny memtable/L0/L1 limits make kills hit flushes and compactions. `bench` reports fillseq, readrandom and scan. README demo section filled in.
**Tests:** CLI tests build the binary and run an 8-iteration crashtest and a small bench; a unit test checks the verifier rejects a DB missing acknowledged ops. A 100-iteration run passed (61k acked ops, 0 lost). Bench: about 650k fillseq/s, 230k readrandom/s without fsync.
**Learned:** with strictly sequential ops the recovered state has only two legal outcomes, which makes the check exact rather than "at least the acked keys".
**Not done:** kill -9 keeps the page cache, so this is not power-loss testing; Day 6 background compaction is still open. Next: fault-injecting FS for power loss, then background flush/compaction.

## 2026-11-01 (later): background compaction
**Changed:** Split compaction into pick/plan, build (no lock) and install (lock, one MANIFEST edit). `Options.Background` starts a compactor goroutine that merges without holding the DB lock; writers stall only when L0 reaches 3x `L0Trigger`; `Close` waits for it and discards unfinished output; `WaitCompaction` exposes idleness/errors. Sync mode is unchanged. `crashtest` now runs with the background compactor. Day 6 is fully ticked.
**Tests:** background data movement and reopen, a 15k-op model check with concurrent Get/Scan readers and periodic reopens, repeated close-while-compacting; all under `-race`. 100-iteration crashtest: 71k acked ops, 0 lost.
**Learned:** flushes only prepend newer L0 tables and only the compactor touches deeper levels, so a single compactor needs no input pinning; the only shared state is the file-number allocator.
**Next:** background flush (writers still block during a flush), a fault-injecting FS for power-loss tests, block cache.

## 2026-11-02: block cache
**Changed:** Added `sstable.Cache`, a byte-bounded LRU of parsed data blocks shared across readers (`Reader.SetCache`), with hit/miss/eviction counters. `db` creates one per DB (`Options.BlockCacheSize`, default 8 MiB, negative disables), attaches it to every table it opens (recovery, flush, compaction output) and exposes `DB.CacheStats()`. Closing a reader drops its blocks so compacted-away tables do not pin memory.
**Tests:** cached and uncached readers return identical results; repeated passes are mostly hits; capacity is respected and evicts; LRU order; blocks dropped on close; 8 concurrent readers under `-race`; DB-level repeated reads produce hits.
**Learned:** because blocks are immutable once parsed and `Get` copies values out, the cache can hand out shared pointers with no refcounting.
**Next:** background flush, snapshots/MVCC reads (needs compaction to pin old versions), power-loss fault-injecting FS.

## 2026-11-03: stats and metrics endpoint
**Changed:** Added `DB.Stats()` (put/delete/get/scan counters, flush and compaction counts, memtable bytes, last seqnum, per-level tables and bytes, block cache stats). New `metrics` package renders it as Prometheus text and exposes an `http.Handler`; `emberkv stats -dir DIR [-listen ADDR]` prints or serves it. Plan stretch item ticked.
**Tests:** DB counters and level gauges against `LevelCounts` after flushes and compactions; handler served over `httptest` with exact sample lines and a format check; passes under `-race`.
**Learned:** op counters as atomics keep `Stats` cheap, but gauges need the read lock for a consistent per-level view.
**Next:** background flush, snapshots (compaction must pin old versions and table readers), power-loss fault-injecting FS.

## 2026-11-03 (later): snapshots
**Changed:** Added `DB.NewSnapshot()` with `Snapshot.Get/Scan/Release`, reading at a pinned seqnum. Compaction now takes the live snapshot seqnums when planned and keeps the newest version of each key plus the newest version visible to every snapshot; bottom tombstones are dropped only when no snapshot predates them. Compaction output is now cut only at user-key boundaries.
**Tests:** isolation against later puts/deletes; two model-checked runs (sync and background, 6000 ops, snapshots taken and released mid-stream) comparing every snapshot's Scan and Get to its model after compaction; reclamation still works with no snapshots; closed-DB behaviour. Pass under `-race`.
**Learned:** the model check's Scan passed while point Get failed: retained versions of one key had been split across two output files and Get probes only one table per level. Cutting at key boundaries fixed it.
**Next:** background flush, power-loss fault-injecting FS.

## 2026-11-04: background flush
**Changed:** With `Options.Background`, a full memtable is now rotated instead of flushed inline: the WAL segment is synced and closed, a new segment and empty memtable take over, and the old memtable becomes the frozen `imm`. A `flushLoop` goroutine writes it to L0 without the DB lock and commits with one MANIFEST edit (`LogNum` = active segment), then deletes the frozen segment. `Get`, `Scan` and `Stats` include `imm`; `Flush()` and `WaitCompaction()` wait for it. Writers stall only if the active memtable fills while an `imm` is still being flushed. `writeTable` now takes the memtable to write.
**Tests:** reads see a frozen memtable; manual flush plus reopen leaves one WAL segment; 15 close-mid-flush rounds lose nothing and leave no `.tmp`; a 15k-op model check with reopens and concurrent readers under `-race`. 60-iteration crashtest (which uses the background path): 34.7k acked ops, 0 lost.
**Learned:** the stall has to happen before the append (while holding the lock via cond wait), not after, or the active memtable grows unbounded behind a slow flush; and the manifest's max-merge semantics for LogNum/NextFile make the out-of-lock install safe.
**Next:** power-loss fault-injecting FS (drop unsynced bytes), then README benchmark refresh.

## 2026-11-04 (later): power-loss simulation
**Changed:** New `vfs` package: `FS`/`File` interfaces, `OS` implementation and `MemFS`, an in-memory filesystem where file bytes are durable only after `Sync` and names only after `SyncDir`. `Crash(rng)` yields the post-power-cut state, optionally keeping random unsynced tail bytes. wal, manifest, sstable and db take an optional FS (`Options.FS`, `OpenFS`, `NewWriterFS`); defaults are unchanged. `wal.Open` now fsyncs the directory when it creates a file.
**Tests:** MemFS unit tests (unsynced bytes, unsynced names/renames/removes, torn tails are prefixes); `TestPowerLossKeepsAcknowledgedWrites` does 40 crash/reopen rounds in sync and background modes and compares to a model. Mutation check: removing the new-file directory sync in the WAL makes it fail with lost keys.
**Learned:** the engine was missing a directory fsync after creating WAL segments, which `kill -9` testing can never reveal.
**Next:** refresh README benchmark numbers; consider injecting write/sync errors in MemFS.

## 2026-11-05: I/O error injection
**Changed:** `MemFS.SetFault` injects errors into Write (leaving a torn half), Sync and SyncDir. The WAL now records the first write/flush/fsync error and fails every later Append/Sync with it, since appending behind a possibly torn record would make replay stop early and drop later acknowledged writes. README bench numbers refreshed (readrandom roughly doubled with the block cache) and error injection documented.
**Tests:** WAL unit test for the sticky failure; DB test with ~1.5% random I/O failures across flushes and compactions (sync and background), power cut after the first failure, recovered state must equal the acknowledged model or that plus the failed op. Passes under `-race`.
**Learned:** the injector and the test's RNG must be separate; background goroutines call the hook concurrently. Not every error means a broken WAL (a failed flush after a good append leaves the log intact), so the DB test does not assert write refusal.
**Next:** surface flush/compaction I/O errors more explicitly (retry or bgErr in sync mode), fault-inject SSTable reads, maybe a compaction/flush mid-failure test.

## 2026-11-06: read-fault injection
**Changed:** `MemFS.SetFault` now also fires on `ReadAt` (op `"read"`), so SSTable block/footer/index reads can fail. README and PLAN updated.
**Tests:** `vfs` unit test for the read fault; `TestReadFaultsSurfaceAsErrors` fails a third of disk reads with the block cache off and checks every `Get`/`Scan` either returns the exact model result or the injected error, with enough of both outcomes, and that data is intact after the fault clears. The existing write-fault test also now sees read faults during compaction and still passes. `check.sh` passes.
**Learned:** no bug found: errors already propagate from `sstable.Reader` through `Get` and the merge iterator's `Err`. The test now guards that.
**Next:** retry/recovery for transient background flush/compaction errors (bgErr is currently sticky until reopen); fault-inject MANIFEST reads at open.
