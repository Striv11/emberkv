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
