# M-U16 Transactional View DDL

M-U16 adds a transactional catalog boundary for session-local SQL views. It is
inspired by Materialize's catalog publication model, but remains an embedded
Go API and does not introduce a durable catalog or a background dataflow.

## Public API

`hatSql.SQLSessionViewChange` describes one named view query. Call
`SQLSession.ApplyViewChanges` with one or more changes to create or replace all
of them atomically:

```go
catalog, err := session.ApplyViewChanges([]hatSql.SQLSessionViewChange{
    {Name: "base", Query: "FROM CACHE('people') SELECT name"},
    {Name: "names", Query: "FROM CACHE('base') SELECT name"},
})
```

`SQLSessionViewCatalog` contains the publication `Version` and sorted,
value-copied `Views`. `ViewCatalogSnapshot` reads the same shape without
changing it, and `CatalogVersion` reads only the version marker.

## Publication Rules

- Every query is parsed and every name is normalized before the session lock is
  acquired.
- Duplicate names in one batch are rejected case-insensitively.
- The candidate dependency graph is built privately and rejected if it has a
  direct or indirect cycle.
- A successful batch replaces the live view map and increments the catalog
  version exactly once, regardless of the number of changes.
- A rejected batch does not alter definitions, dependent views, or the version.
- Returned definitions and dependency slices are independent copies.
- `CREATE OR REPLACE VIEW name AS query` is supported by `SQLSession.Execute`.
  Existing `CREATE VIEW` behavior remains compatible and uses the allocation
  fast path for one-view changes.

The version is a publication marker for callers coordinating session-local
metadata. It is not a durable WAL sequence, a cross-session transaction, or a
promise that a query already executing will mix no definitions from a later
publication. Durable replication, persistence, and full query isolation remain
caller-owned.

## Example

```sql
CREATE OR REPLACE VIEW names AS
FROM CACHE('people') SELECT name;
```

For a batch, use the Go API when several dependent definitions must become
visible together. A failed cyclic replacement leaves the previously published
view graph available.

## Verification

The focused contract covers sorted snapshots, dependent replacement, cycle
rejection, duplicate-name rejection, snapshot isolation, version increments,
and SQL `CREATE OR REPLACE VIEW` execution. The standard commands are:

```text
make test-mu016-transactional-views
make test-mu016-transactional-view-package
make race-mu016-transactional-views
make vet-mu016-transactional-views
make benchmark-mu016-baseline
make benchmark-mu016-transactional-views
```

## Benchmark

Five runs used `GOMAXPROCS=1` and `-benchtime=500ms` on Linux/amd64, AMD
Ryzen 9 5950X:

| Path | Median ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| Pre-change one-view `CreateView` | 2,174 | 2,888 | 12 |
| M-U16 one-view compatibility path | 2,121 | 2,904 | 12 |
| M-U16 two-view atomic batch | 5,317 | 5,320 | 22 |
| M-U16 two-view replacement batch | 8,910 | 10,432 | 41 |

The compatibility path is approximately 2% faster with unchanged allocation
count and 16 additional bytes per session object. Multi-view batch and
replacement costs are new operations and are reported separately rather than
presented as a false comparison with the old single-view API.
