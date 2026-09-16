# CH-U11: Automatic Data-Skipping-Index Selection

CH-U11 adds workload-driven selection for the existing JSON path Bloom
data-skipping index. The selector is intentionally advisory: it recommends
compatible indexes from bounded slow-query observations, but it does not
create indexes, rebuild sources, or alter the SQL planner by itself.

That boundary keeps the default safe. An index build consumes source CPU and
retained metadata, and an operator may need to approve that cost per cache,
tenant, or deployment.

## Supported Workload

The existing `SQLIndexAdvisor` records a candidate when all of these are true:

- the query is a successful slow single-source `CACHE` query;
- no existing index appears in the execution metrics;
- the predicate is an `AND` conjunction containing `JSON_VALUE(field, literal_path) = literal`;
- the JSON path is a valid canonical path;
- the advisor still has bounded capacity.

Range predicates, `OR` branches, dynamic paths, joins, failed queries, and
queries already using an index are not recommended. The executor still
rechecks the original predicate after a skip-index candidate is returned, so
Bloom false positives cannot change results.

## API

`SQLIndexAdvisor.SkipIndexRecommendations(limit)` returns candidates ordered
by total observed elapsed time, then average elapsed time, query count, source
key, document field, and path. A positive limit bounds the returned slice;
zero returns all retained candidates.

The recommendation contains only:

```go
type SQLJSONPathSkipIndexRecommendation struct {
    Key            string
    Field          string
    Path           string
    SlowQueries    uint64
    TotalElapsed   time.Duration
    AverageElapsed time.Duration
}
```

No SQL text, parameter, predicate value, or source row is retained. The
advisor is opt-in through `QueryOptions.IndexAdvisor`; a nil advisor keeps the
existing query path and allocates no selector state.

## Example

```go
advisor := hatSql.NewSQLIndexAdvisor(128)
options := hatSql.QueryOptions{
    IndexAdvisor:       advisor,
    SlowQueryThreshold: 10 * time.Millisecond,
}

_, err := hatSql.ExecuteQueryParameters(
    ctx,
    "FROM CACHE('people') AS person WHERE JSON_VALUE(person.doc, '$.profile.city') = 'Singapore' SELECT person.doc",
    trie,
    nil,
    options,
)
if err != nil {
    return err
}

for _, recommendation := range advisor.SkipIndexRecommendations(16) {
    if err := trie.CreateSQLJSONPathSkipIndex(hatCache.SQLJSONPathSkipIndexSpec{
        CacheKey: recommendation.Key,
        Paths:    []string{recommendation.Path},
    }); err != nil {
        return err
    }
    if err := trie.ScheduleSQLJSONIndexRebuild(recommendation.Key, recommendation.Path); err != nil {
        return err
    }
}
```

The example is deliberately an explicit operator action. After a source
change, use the existing maintenance worker or
`RunScheduledSQLJSONIndexRebuilds`; index freshness remains visible through
`SQLJSONIndexMaintenanceStats`.

## Persistence

`SQLIndexAdvisor.Save` writes skip-index observations in snapshot version 3.
`Load` accepts versions 1 and 2 for backward compatibility. The snapshot is
still bounded and rejects invalid paths, duplicate candidates, oversized
entries, and capacity violations before replacing live state.

## Measurements

Commands:

```sh
make benchmark-chu11-before-c251
make benchmark-chu11-c251
```

Five samples were run from clean archives on Linux/amd64, AMD Ryzen 9 5950X.
The ranges below are the observed minimum and maximum samples.

| Workload | Clean HEAD | CH-U11 | Result |
|---|---:|---:|---|
| Existing advisor disabled | 4.60-4.87 us/op; 5,936 B/op; 23 allocs | 4.73-5.01 us/op; 5,936 B/op; 23 allocs | No measurable default-path regression |
| Existing advisor, non-covering candidate | 12.98-14.57 us/op; 9,659 B/op; 98 allocs | 12.30-12.88 us/op; 9,659-9,660 B/op; 98 allocs | No allocation regression after lazy collection |
| Existing advisor, covering candidate | 14.34-14.83 us/op; 10,430 B/op; 105 allocs | 13.33-14.42 us/op; 10,430 B/op; 105 allocs | Within run variance |
| Skip selector, 128 candidates, top 16 | Not available | 14.47-16.13 us/op; 9,624 B/op; 4 allocs | Explicit reporting cost only |
| JSON query, advisor disabled | Not available | 9.64-10.41 us/op; 8,664 B/op; 52 allocs | Reference |
| JSON query, advisor enabled | Not available | 16.25-17.92 us/op; 12,445 B/op; 133 allocs | Opt-in observation overhead |

The selector does not make a query faster until an operator creates a
recommended skip index. Its benefit is reducing guesswork and prioritizing
the indexes with the largest observed workload cost. The index itself remains
bounded by the existing rows-per-segment, bits-per-segment, admission, and
maintenance controls.

## Verification

The focused test was written before implementation. These clean-archive
targets pass:

```sh
make test-chu11-clean-c251
make test-chu11-package-clean-c251
make race-chu11-clean-c251
make vet-chu11-clean-c251
```

The full repository test was not run from the shared worktree because another
session currently has `hat/hatSql/asof_join.go` deleted; the clean package
verification restores the committed file and passes the complete `hatSql`
package.
