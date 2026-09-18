# TR-35 Typed-Table MVCC Snapshot Views

Status: implemented as an opt-in typed-table feature.

Create a `TypedTable` with `TypedTableMVCCOptions{Enabled: true}` to enable
historical versions. `Snapshot()` captures the current sequence,
`SnapshotAt(sequence)` opens a stable historical view, and
`CompactMVCCThrough(sequence)` removes historical links older than the chosen
frontier. `TypedTableSnapshot` implements the normal SQL source and columnar
source resolver contracts, so a query can continue reading a stable view while
the mutable table advances.

MVCC is disabled by default. Compaction rejects new snapshots older than its
frontier, while snapshots that already hold immutable version heads remain
valid. The feature is scoped to typed tables; it does not provide a global
multi-source transaction snapshot or arbitrary `HatTrie` historical versions.

The current benchmark shows the tradeoff: MVCC writes add two allocations and
about 18% bytes in the fixture, while materializing current snapshot rows is
about 10% slower with nearly unchanged bytes. Run
`make test-sql-typed-table-mvcc`, `make test-race-sql-typed-table-mvcc`, and
`make benchmark-sql-typed-table-mvcc` to reproduce the checks documented in
[BENCHMARK.md](BENCHMARK.md#opt-in-typed-table-mvcc-snapshots).
