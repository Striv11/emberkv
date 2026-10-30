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
