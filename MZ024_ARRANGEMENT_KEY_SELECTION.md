# MZ-024 Automatic Arrangement Key Selection

This feature adopts a small part of Materialize's arrangement-selection idea:
the SQL planner can score existing reusable arrangements against the query
fields used by `WHERE`, `GROUP BY`, `ORDER BY`, and joins.

It is deliberately advisory. It does not create, drop, hydrate, or rewrite an
arrangement, and it does not change ordinary query execution. The selection is
visible in `EXPLAIN` when the source resolver implements
`SQLArrangementMetadataResolver`.

## Importable API

Call `hatSql.RecommendSQLArrangement` when an application owns arrangement
creation or wants to make an explicit policy decision:

```go
recommendation := hatSql.RecommendSQLArrangement(
    []hatSql.SQLArrangementMetadata{
        {
            Key:         "region,day",
            Kind:        "sorted-aggregate",
            Fields:      []string{"region", "day"},
            Reused:      true,
            Cardinality: 365,
            MemoryBytes: 64 * 1024,
        },
    },
    hatSql.SQLArrangementWorkload{
        FilterFields:  []string{"tenant_id"},
        GroupByFields: []string{"region"},
        OrderByFields: []string{"day"},
    },
)
```

`Fields` is optional for compatibility with existing metadata providers. When
it is absent, the selector extracts bounded field tokens from `Key`. Explicit
fields are preferred because they avoid guessing from a display key.

The result contains the selected key, kind, deterministic score, and a short
reason. No result is returned when none of the candidates matches a workload
field. Ties prefer more matched fields, reused arrangements, lower known
memory, lower known cardinality, and then stable key/kind order.

## EXPLAIN Output

`EXPLAIN` and `EXPLAIN PIPELINE` keep listing all arrangement metadata. The
selected entry additionally has:

- `recommended: true`
- `match_score`
- `recommendation`, such as `matched GROUP BY, ORDER BY fields`

This keeps the existing arrangement list and makes the choice machine-readable
without inventing a new execution mode. A resolver error or missing metadata
continues to produce a valid plan without a recommendation.

## Bounds And Defaults

The selector is off unless arrangement metadata is supplied to `EXPLAIN`; no
configuration is required and no ordinary read/write path pays this cost. The
existing 64-entry and 256-byte metadata bounds remain in force. Selection
normalizes at most 32 fields per workload dimension and arrangement metadata
retains at most 16 explicit fields.

The benchmark measures the diagnostic selector directly. It is not a claim
that a recommendation alone accelerates a query: the application must expose
or acquire the recommended arrangement. Ordinary execution remains byte-for-
byte behaviorally unchanged.
