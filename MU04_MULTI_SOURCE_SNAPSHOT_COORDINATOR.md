# M-U04 Multi-Source Snapshot Coordinator

`hatSql.SQLMultiSourceSnapshotCoordinator` captures several independent SQL
source snapshots concurrently and publishes them as one immutable view. It is
an opt-in control-plane API; ordinary SQL reads do not pay for it unless the
caller resolves through the coordinated view.

## Why It Exists

Per-source snapshot ingestion can authenticate and checkpoint one source, but
joining two independently captured sources can otherwise expose different
initial points in time. The coordinator gives every source one publication
generation while retaining each source's exact `SnapshotID` and offsets for
starting its live tail.

## Capture And Recovery

```go
coordinator, err := hatSql.NewSQLMultiSourceSnapshotCoordinator(
    hatSql.SQLMultiSourceSnapshotCoordinatorOptions{MaxSources: 16},
)
if err != nil {
    return err
}

result, err := coordinator.CaptureWithCheckpoint(
    ctx,
    []hatSql.SQLMultiSourceSnapshotRequest{
        {Source: "orders-db", Key: "orders", Kind: "POSTGRES", Provider: ordersProvider},
        {Source: "customer-cdc", Key: "customers", Kind: "CDC", Provider: customersProvider},
    },
    checkpointStore,
    hatSql.SQLMultiSourceSnapshotCaptureOptions{RequireSnapshotIDs: true},
)
if err != nil {
    return err
}

orders, err := result.View.ResolveSQLSource("POSTGRES", "orders")
```

The checkpoint store's `Commit` receives the complete multi-source payload in
one call. A source authentication, provider, validation, context, or commit
failure leaves the previously published coordinator view unchanged. On a
subsequent call, a valid stored snapshot is restored without calling any
provider, so upstream credentials are not needed for recovery.

Source IDs must be unique inside one coordinated snapshot. Sources are sorted
by source, key, and kind before publication. Rows, metadata, offsets, and
checkpoint payloads are detached copies; callers can mutate their own input
without changing the published view.

## Bounds And Security

The default maximum is 16 sources. Per-source row, offset, and page limits
default to the existing bounded external-snapshot limits and can be lowered in
`SQLMultiSourceSnapshotCoordinatorOptions`. The coordinator never stores
provider credentials. Checkpoint implementations should authenticate and
authorize their storage location, use restrictive file permissions or an
encrypted backend, and commit atomically.

`RequireSnapshotIDs: true` should be used when a live tail must start at an
exact upstream position. The returned metadata contains one source snapshot ID
and sorted `(partition, offset)` values per source.

## Benchmark

Commands:

```sh
make benchmark-m054-baseline
make benchmark-m054
```

Five samples on Linux/amd64, AMD Ryzen 9 5950X. The control resolves one of
two sources through a prebuilt map and performs the same deep row clone as the
coordinated view. Capture and recovery each operate on two one-row sources.

| Workload | ns/op samples | Median ns/op | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| Direct two-source resolver control | 257.0, 253.5, 248.8, 251.7, 251.6 | 251.6 | 344 | 3 |
| Coordinated capture, two sources | 12,670, 12,845, 12,539, 12,972, 12,317 | 12,670 | 8,416 | 81 |
| Coordinated checkpoint recovery | 5,244, 5,254, 5,103, 5,108, 5,067 | 5,108 | 5,872 | 57 |
| Coordinated view resolve | 280.4, 280.6, 281.4, 274.9, 280.8 | 280.6 | 344 | 3 |

The steady-state coordinated resolve is about 1.12x the direct control's CPU
time with identical measured memory and allocation counts. The capture and
recovery rows are control-plane costs, not per-row update costs; capture pays
for authentication dispatch, bounded collection, deep copies, validation, and
one checkpoint commit.
