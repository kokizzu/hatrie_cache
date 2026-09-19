# M-U03 External Snapshot Ingestion

`hatSql.SQLExternalSnapshotIngestor` is the dependency-free boundary for
bootstrapping a SQL source from Kafka, PostgreSQL, CDC, or another external
system. The repository does not force a broker or database driver into the
library. A connector adapter implements `SQLExternalSnapshotProvider`, and the
caller chooses the durable `SQLExternalSnapshotCheckpointStore`.

## Contract

The provider must:

1. Authenticate before reading upstream data.
2. Stream bounded pages through `SQLExternalSnapshotSink.AppendRows`.
3. Return source identity, a snapshot ID, and source-partition offsets after
   all pages have been accepted.

The ingestor then validates identity and offset uniqueness, clones rows, and
publishes one checkpoint. It implements `SourceResolver`, so SQL reads see a
stable in-memory snapshot and never call the external provider.

```go
ingestor, err := hatSql.NewSQLExternalSnapshotIngestor(
    hatSql.SQLExternalSnapshotIngestorOptions{
        Source: "orders-source",
        Key:    "orders",
        Kind:   "POSTGRES",
    },
)
if err != nil {
    return err
}

result, err := ingestor.IngestSnapshotWithCheckpoint(
    ctx,
    provider,
    checkpointStore,
    hatSql.SQLExternalSnapshotIngestOptions{RequireSnapshotID: true},
)
if err != nil {
    return err
}
_ = result
```

`SQLExternalSnapshotProvider` is deliberately page-oriented:

```go
type SQLExternalSnapshotProvider interface {
    Authenticate(context.Context) error
    Snapshot(context.Context, SQLExternalSnapshotSink) (SQLExternalSnapshotMetadata, error)
}
```

The metadata offsets are connector-neutral strings. A Kafka adapter can use a
partition number, PostgreSQL can use a WAL position, and a CDC adapter can use
its stream or transaction boundary. The offset source must match the ingestor
source, and duplicate source/partition entries are rejected.

## Recovery And Cutover

`IngestSnapshotWithCheckpoint` first checks the checkpoint store. A valid
checkpoint is restored without contacting the upstream provider, so a restart
does not require credentials or a live source before local recovery. When no
checkpoint exists, authentication and snapshot paging occur, then the complete
row set and offsets are installed under one lock and committed as one durable
payload. A failed checkpoint commit restores the exact previous in-memory
state.

The snapshot ID is optional for compatibility, but production adapters should
set `RequireSnapshotID: true`. It makes a point-in-time boundary auditable and
prevents an anonymous snapshot from being mistaken for a recovered one.

The default bounds are deliberately finite:

| Limit | Default | Absolute maximum |
| --- | ---: | ---: |
| Rows | 1,000,000 | 10,000,000 |
| Source offsets | 100,000 | 1,000,000 |
| Rows per provider page | 10,000 | 100,000 |

`AppendRows` rejects a page or total snapshot that exceeds its bound before it
is copied. Providers should page from the upstream system instead of building
an unbounded response in memory.

The checkpoint store is an application boundary. Its `Commit` implementation
must atomically publish metadata and rows, normally by writing a temporary
record and replacing the active checkpoint after integrity verification. The
ingestor does not persist credentials, connection strings, or provider error
text.

## Failure Behavior

| Failure | Result |
| --- | --- |
| Authentication failure | Provider is not called; no state changes. |
| Context cancellation | Provider/store work stops at the next boundary; no state changes before install. |
| Wrong source, key, or kind | Snapshot is rejected before install. |
| Duplicate or foreign offset | Snapshot is rejected before install. |
| Page/row/offset bound exceeded | The sink rejects the page; the checkpoint is not attempted. |
| Checkpoint commit failure | In-memory rows, offsets, ID, and generation roll back. |
| Existing checkpoint | It is restored and upstream authentication is skipped. |
| Existing in-memory state without checkpoint | Replacement is rejected unless `AllowReplaceExisting` is explicit. |

The caller still owns the connector-specific tail loop. After the snapshot
offset is durably committed, it should begin change consumption strictly after
that offset and use the same checkpoint discipline for subsequent progress.

## Verification

```text
make test-m053
ok   hatrie_cache/hat/hatSql  0.005s

make verify-m053
ok   hatrie_cache/hat/hatSql  0.583s
ok   hatrie_cache/hat/hatSql  2.223s
```

The focused tests cover paged ingestion, authentication ordering, checkpoint
recovery without upstream access, row/page bounds, deterministic offsets,
duplicate offsets, identity rejection, deep-copy isolation, cancellation, and
atomic rollback after a failed checkpoint commit.

## Benchmark

All runs used five samples on an AMD Ryzen 9 5950X with Go `-benchmem`.
The control path is the existing `VirtualSources.ResolveSQLVirtualSource`
resolver over 128 rows. The feature read path uses the new ingestor over the
same row shape. Bootstrap uses one 128-row page and a no-op checkpoint store.

| Path | Clean origin median | Feature median | Feature memory | Feature allocs | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| Existing virtual-source control | 38,521 ns/op | 39,703 ns/op | 44,160 B/op | 257 allocs/op | 1.03x slower observed; same memory/allocations |
| New ingestor resolver | n/a | 44,120 ns/op | 44,160 B/op | 257 allocs/op | New source read path |
| New snapshot bootstrap | n/a | 172,797 ns/op | 221,882 B/op | 1,293 allocs/op | New control-plane path |

The 1.03x control delta is within normal run variance and is not a feature
speedup or a measured allocation regression; the control implementation is
unchanged. Bootstrap costs more because it validates and clones pages, then
clones the committed checkpoint payload to preserve atomic ownership. That
transient memory is bounded by the configured row/page limits and is paid only
at snapshot cutover, not on ordinary SQL reads.

Raw clean-origin control output:

```text
BenchmarkSQLExternalSnapshotControl-32 31574 38594 ns/op 44160 B/op 257 allocs/op
BenchmarkSQLExternalSnapshotControl-32 29942 38521 ns/op 44160 B/op 257 allocs/op
BenchmarkSQLExternalSnapshotControl-32 30085 38211 ns/op 44160 B/op 257 allocs/op
BenchmarkSQLExternalSnapshotControl-32 31456 37478 ns/op 44160 B/op 257 allocs/op
BenchmarkSQLExternalSnapshotControl-32 31219 39847 ns/op 44160 B/op 257 allocs/op
```

Raw feature output:

```text
BenchmarkSQLExternalSnapshotControl-32 36525 36908 ns/op 44160 B/op 257 allocs/op
BenchmarkSQLExternalSnapshotControl-32 31890 34746 ns/op 44160 B/op 257 allocs/op
BenchmarkSQLExternalSnapshotControl-32 33489 39703 ns/op 44160 B/op 257 allocs/op
BenchmarkSQLExternalSnapshotControl-32 28386 42067 ns/op 44160 B/op 257 allocs/op
BenchmarkSQLExternalSnapshotControl-32 26712 46428 ns/op 44160 B/op 257 allocs/op
BenchmarkSQLExternalSnapshotResolve-32 26242 45812 ns/op 44160 B/op 257 allocs/op
BenchmarkSQLExternalSnapshotResolve-32 27756 44202 ns/op 44160 B/op 257 allocs/op
BenchmarkSQLExternalSnapshotResolve-32 28069 44120 ns/op 44160 B/op 257 allocs/op
BenchmarkSQLExternalSnapshotResolve-32 37256 33018 ns/op 44160 B/op 257 allocs/op
BenchmarkSQLExternalSnapshotResolve-32 36052 33022 ns/op 44160 B/op 257 allocs/op
BenchmarkSQLExternalSnapshotIngest-32 6694 172797 ns/op 221882 B/op 1293 allocs/op
BenchmarkSQLExternalSnapshotIngest-32 6990 175577 ns/op 221882 B/op 1293 allocs/op
BenchmarkSQLExternalSnapshotIngest-32 6864 170938 ns/op 221882 B/op 1293 allocs/op
BenchmarkSQLExternalSnapshotIngest-32 7497 169535 ns/op 221883 B/op 1293 allocs/op
BenchmarkSQLExternalSnapshotIngest-32 7450 174334 ns/op 221883 B/op 1293 allocs/op
```
