# M-U15 SQL Source Status Catalog

M-U15 adds a stable SQL relation for source lifecycle and progress status. It
uses the source frontier primitives already present in `hatMetrics` and the
optional `hatSql.SQLSourceFrontierResolver` contract, while allowing a caller
to publish one coherent richer status snapshot.

## Configure A Snapshot

Add `SourceStatuses` to `hatSql.Catalog` for a static snapshot, or set
`CatalogResolver.SourceStatus` for a live callback that returns one snapshot per
metadata query:

```go
resolver := hatSql.CatalogResolver{
    Catalog: hatSql.Catalog{
        Version: 17,
        Sources: []hatSql.CatalogSource{
            {Namespace: "public", Name: "orders", Kind: "CACHE"},
        },
    },
    SourceStatus: hatSql.CatalogSourceStatusResolverFunc(func() ([]hatSql.CatalogSourceStatus, error) {
        return []hatSql.CatalogSourceStatus{
            {
                Namespace: "public",
                Source:    "orders",
                Kind:      "CACHE",
                State:     hatSql.CatalogSourceStateRunning,
                Available: true,
                Frontier:  8,
                Observed:  10,
                ErrorCode: "timeout",
            },
        }, nil
    }),
}
```

The callback is called once for a relation query, so all returned rows belong
to the same status snapshot. Callback rows override static rows with the same
namespace and source. Catalog sources without an explicit status are included
as `unknown`.

When no explicit status exists, `CatalogResolver` checks a wrapped source's
existing `SQLSourceFrontierResolver`. An available ready frontier becomes
`ready`; an available non-ready frontier becomes `catching_up`. An unavailable
frontier remains `unknown`. This reuses existing source progress state instead
of maintaining a second frontier tracker.

## Relation And Commands

Query the read-only virtual relation:

```text
FROM CACHE('information_schema.source_status')
SELECT namespace, source, state, frontier, observed, lag, error_code
ORDER BY namespace, source
```

The shorthand forms are equivalent:

```text
SHOW SOURCE STATUS
SHOW SOURCE_STATUS
```

The relation columns are:

| Column | Meaning |
| --- | --- |
| `catalog_version` | Version of the containing catalog snapshot; legacy zero is exposed as `1`. |
| `namespace` | Source namespace. |
| `source` | Logical source name. |
| `kind` | Source resolver kind, such as `CACHE`. |
| `state` | `unknown`, `starting`, `running`, `ready`, `catching_up`, `stopped`, `failed`, or `degraded`. |
| `available` | Whether a current source observation is available. |
| `ready` | Whether the source is ready for the caller's frontier contract. |
| `frontier` | Latest source frontier when available. |
| `observed` | Consumer or global observed frontier. |
| `lag` | `observed - frontier` when observed is ahead; otherwise zero. |
| `error_code` | Optional bounded identifier for a failure or degraded condition. |

The status relation is owned by `CatalogResolver`; optional columnar,
partition, ordering, index-diagnostic, cardinality, and frontier resolution
contracts are not forwarded to an application resolver for this virtual source.

## Safety And Consistency

Only identifier-shaped error codes containing lowercase ASCII letters, digits,
`_`, `-`, or `.` and at most 64 bytes are accepted. Raw error messages are not
stored or returned by this relation, which avoids leaking credentials, query
text, or connection strings. Invalid states, unsafe error codes, and
inconsistent non-zero lag values return `ErrCatalogSourceStatusInvalid` rather
than exposing ambiguous status.

Status rows are sorted by namespace, source, and kind. The catalog version is
attached to every row. A source status callback should return a bounded list;
the relation is a snapshot interface and does not retain historical rows.

## Benchmark

Five standalone samples on an AMD Ryzen 9 5950X, using
`make benchmark-mu015-source-status-catalog`:

| Workload | Median ns/op | B/op | Allocs/op | Relative |
| --- | ---: | ---: | ---: | ---: |
| Pre-change three-source catalog | 11,230 | 9,632 | 60 | 1.00x |
| Post-change three-source catalog | 10,999 | 9,680 | 60 | 1.02x measured, within noise |
| Three-source status catalog | 20,131 | 16,336 | 87 | 1.79x CPU, new metadata path |

The existing source query did not regress meaningfully. The new status query
adds callback normalization and eleven projected columns, so its extra CPU and
allocation cost is paid only when status metadata is requested; normal data
reads and sources without a status query are unchanged.

Raw output is retained in [BENCHMARK.md](BENCHMARK.md#mu-015-sql-source-status-catalog).
