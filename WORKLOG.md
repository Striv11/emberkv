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
