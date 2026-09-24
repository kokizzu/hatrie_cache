# M041 Arrangement Recovery Bundle

M041 closes the arrangement-only recovery gap for callers that maintain more
than one aggregate or join catalog. The existing per-catalog checkpoints remain
the source of truth; the bundle only supplies deterministic routing,
validation, and one release/rollback boundary.

## API

```go
checkpoint, err := hatSql.CaptureTypedTableArrangementRecovery(
    []*hatSql.TypedTableAggregateArrangements{aggregateCatalog},
    []*hatSql.TypedTableJoinArrangements{joinCatalog},
)
if err != nil {
    return err
}

lease, err := hatSql.RestoreTypedTableArrangementRecovery(
    []*hatSql.TypedTableAggregateArrangements{newAggregateCatalog},
    []*hatSql.TypedTableJoinArrangements{newJoinCatalog},
    checkpoint,
)
if err != nil {
    return err
}
defer lease.Release()
```

`TypedTableArrangementRecoveryCheckpoint` is exported and can be encoded with
the existing JSON or binary storage choice. It contains aggregate and join
checkpoint slices and a versioned envelope. `AggregateArrangements()` and
`JoinArrangements()` expose the restored leases when the caller needs to query
the restored state directly.

## Recovery Contract

- Capture does not reread source rows. Each nested checkpoint must still match
  the exact current source sequence.
- Catalogs are routed by persisted table names and join input names. Duplicate
  catalog identities, missing catalogs, duplicate arrangement definitions, and
  unsupported envelope versions are rejected before mutation.
- Restore targets must be empty. This prevents an accidental merge with stale
  in-memory state and makes the recovery boundary explicit.
- If any later catalog or nested checkpoint fails, leases created earlier in the
  same call are released. The target catalogs remain empty.
- `Release` is idempotent. The total aggregate-plus-join checkpoint count is
  bounded by `MaxTypedTableArrangementRecoveryCheckpoints`.
- Aggregate capture writes the canonical stored group key even when the
  aggregate has not materialized its lazy ordering keys. This keeps a captured
  checkpoint restorable after a row-ordering call or a cold restart.

The caller owns durable file/object storage, source lifecycle, catalog
construction, and replay of changes that occurred after the captured source
sequence. The default query and write paths are unchanged because the API is
opt-in.

## Measured Cost

The focused benchmark compares the previous manual sequence of
`CaptureCheckpoints`/`RestoreCheckpoints` calls with the bundle coordinator on
one aggregate and one join arrangement over 4,096 aggregate rows and 1,024
rows per join input. The bundle is a control-plane API, not a row-update hot
path. Capture stayed within 0.6% of manual orchestration; restore stayed within
normal benchmark noise and used 1,248 more bytes and 19 more allocations per
operation in this fixture. The encoded bundle was 1,283,432 bytes. Raw samples
and commands are in [BENCHMARK.md](BENCHMARK.md#m-u05-arrangement-recovery-bundle).
