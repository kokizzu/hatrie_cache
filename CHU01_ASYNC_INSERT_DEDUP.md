# CH-U01 Durable Async-Insert Deduplication

`hatPipeline.AsyncInsertDeduplicator` is an opt-in insert-ID ledger for
ClickHouse-style asynchronous inserts. It admits a `(source, insert ID)` pair
once for a bounded retry window and hashes the payload to detect accidental ID
reuse with different data.

## Usage

```go
store, err := hatPipeline.NewAsyncInsertDedupFileStore(
    hatPipeline.AsyncInsertDedupFileStoreOptions{Path: "/var/lib/hatrie/async-inserts.log"},
)
if err != nil {
    return err
}
ledger, err := hatPipeline.NewAsyncInsertDeduplicator(
    hatPipeline.AsyncInsertDeduplicatorOptions{
        Store:    store,
        Capacity: 100000,
        TTL:      24 * time.Hour,
    },
)
if err != nil {
    return err
}

decision, err := ledger.Accept(ctx, "orders", insertID, payload)
switch {
case err != nil:
    return err
case decision == hatPipeline.AsyncInsertDuplicate:
    return nil // retry is already durably admitted
case decision == hatPipeline.AsyncInsertAccepted:
    return batcher.Submit(ctx, value)
}
```

The caller must call `Accept` before submitting the corresponding value to an
`AsyncBatcher`. The ordinary batcher remains unchanged and has no dedup memory,
hashing, or persistence overhead unless this ledger is explicitly constructed.

The default capacity is 100,000 live IDs, the default TTL is 24 hours, and a
file ledger is limited to 64 MiB. The map grows with admitted IDs rather than
preallocating the full capacity. `Compact` removes expired records from a file
ledger; it is explicit so normal admission does not perform an O(n) rewrite.

## Durability and safety

The file store appends a CRC32C-protected binary record and calls `fsync`
before `Accept` returns success. Reopening the store replays records and
ignores expired entries. Compaction writes a private `0600` temporary file,
fsyncs it, atomically renames it, and syncs the directory. Symlink paths,
oversized files, malformed frames, checksum failures, invalid IDs, and
conflicting payload digests are rejected.

The guarantee is process-local to one ledger file. Do not share one file among
independent writers without an external ownership/fencing policy.

## Measurements

AMD Ryzen 9 5950X, linux/amd64, Go benchmark with five samples per case:

| Path | Median | Memory | Allocations | Comparison |
| --- | ---: | ---: | ---: | --- |
| Memory-only duplicate admission | 181.9 ns/op | 0 B/op | 0 allocs/op | Comparable to the 200.5 ns/op clean async-submit baseline |
| Durable file append plus fsync | 709,979 ns/op | 830 B/op | 9 allocs/op | About 3,900x slower than memory-only admission |
| Default `AsyncBatcher.Submit` after feature | 194.5 ns/op | 0 B/op | 0 allocs/op | No feature-path allocation; within benchmark variation of the 200.5 ns/op clean baseline |

The durable path is therefore not a throughput optimization. It is an
explicit correctness option for retry-heavy or lower-rate asynchronous
inserts. High-rate users should keep the ledger in memory or use a larger
durability batch/transaction owned by their storage layer rather than paying
one fsync per ID.
