# C250: Retry-Safe Async Insert Identities

Status: adopted and verified.

ClickHouse-style async insert identities are already implemented in the public
`hatPipeline` package. The feature gives an ingestion boundary a bounded
`source` plus client-provided `ID` ledger, so retries can be recognized by
later asynchronous stages without replaying the same payload.

The implementation predates this inspiration item in commit
`76b512b1` (`add durable async insert deduplication`). This change records the
adoption and its measurements; it does not add a second deduplication system.

## Semantics

`AsyncInsertDeduplicator.Accept` hashes the payload with SHA-256 and admits a
`source`/`ID` pair only once during its configured retry window.

- The first payload returns `AsyncInsertAccepted`.
- The same identity and payload return `AsyncInsertDuplicate` without applying
  the payload again.
- Reusing an identity with a different payload returns `ErrAsyncInsertConflict`.
- The durable store is appended and synced before the in-memory admission is
  published. A failed durable append therefore leaves the identity retryable.
- `Compact` and expiry remove old records explicitly; normal `Accept` calls do
  not rewrite the full durable ledger.

The identity must be carried by the caller through its asynchronous stages.
This is intentionally opt-in: existing `AsyncBatcher` and unkeyed async
insert callers retain their behavior unless they call `Accept`.

## Usage

An in-memory ledger is suitable when the retry window only needs to survive
process-local failures:

```go
deduplicator, err := hatPipeline.NewAsyncInsertDeduplicator(
    hatPipeline.AsyncInsertDeduplicatorOptions{
        Capacity: 100_000,
        TTL:      24 * time.Hour,
    },
)
if err != nil {
    return err
}

decision, err := deduplicator.Accept(ctx, "orders", requestID, payload)
if err != nil {
    return err
}
if decision == hatPipeline.AsyncInsertDuplicate {
    return nil
}
// Enqueue payload for the remaining asynchronous stages.
```

For retry survival across process restarts, supply the optional append-only
file store. Its parent directory must already exist:

```go
store, err := hatPipeline.NewAsyncInsertDedupFileStore(
    hatPipeline.AsyncInsertDedupFileStoreOptions{
        Path: "/var/lib/hatrie/async-insert-ledger",
    },
)
if err != nil {
    return err
}
deduplicator, err := hatPipeline.NewAsyncInsertDeduplicator(
    hatPipeline.AsyncInsertDeduplicatorOptions{
        Store: store,
    },
)
```

## Bounds And Durability

The defaults are bounded and can be overridden within hard limits:

| Setting | Default | Hard limit |
| --- | ---: | ---: |
| Live identities | 100,000 | 1,048,576 |
| Retry TTL | 24 hours | Positive duration |
| Source length | 256 bytes | 256 bytes |
| Identity length | 512 bytes | 512 bytes |
| Durable ledger | 64 MiB | 1 GiB |

The durable format uses `HAD1` frames with CRC32C checksums. Loading rejects
truncated, malformed, or checksum-invalid frames. The file path rejects
symlinks, appends are fsynced, and compaction writes a bounded temporary file,
renames it atomically, and syncs its directory. These checks are important
because insert IDs and ledger paths are caller-controlled inputs.

C204's projection metadata carries the identity as correlation data through
incremental materialized-view work. It is not a second deduplication ledger;
the C250 admission ledger remains the place where retry acceptance is decided.

## Measurements

The existing focused benchmark ran five 200 ms samples on an AMD Ryzen 9
5950X, Linux amd64. These are absolute opt-in costs, not a before/after claim:
the feature was already present before this inspiration item was cataloged.

| Operation | Median time | Heap | Allocs | Interpretation |
| --- | ---: | ---: | ---: | --- |
| In-memory `Accept` | 179.2 ns/op | 0 B/op | 0 allocs/op | Suitable for the low-latency admission path |
| Durable file `Append` | 1,083,536 ns/op | 829 B/op | 9 allocs/op | Includes fsync; high variance is expected |

The five durable append samples were `1,256,912`, `4,030,487`, `813,932`,
`1,083,536`, and `871,817 ns/op`. The fsync cost is the deliberate tradeoff
for restart durability, so the durable store should be selected when retry
survival matters more than per-insert latency.

## Verification

The focused test suite covers duplicate acceptance, payload conflicts, expiry,
capacity, restart recovery, concurrent admission, corruption, bounds,
symlink rejection, and durable failure ordering.

```text
make test-chu01-async-dedup
make race-chu01-async-dedup
make vet-chu01-async-dedup
make benchmark-chu01-async-dedup
```

The raw benchmark output is tracked in [BENCHMARK.md](BENCHMARK.md#c250-retry-safe-async-insert-identities).
