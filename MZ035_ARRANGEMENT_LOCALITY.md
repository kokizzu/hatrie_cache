# MZ-035 Arrangement Locality Hints

Hatrie Cache now accepts optional locality metadata when selecting an existing
SQL arrangement for `EXPLAIN`. This is inspired by Materialize's awareness of
arrangement placement, but is deliberately advisory: it does not move data,
create shards, assign workers, or change query execution.

## API

Expose a bounded locality label on arrangement metadata and pass the locality
preferred by the query workload:

```go
candidates := []hatSql.SQLArrangementMetadata{
	{Key: "user_id:west", Fields: []string{"user_id"}, Locality: "us-west"},
	{Key: "user_id:east", Fields: []string{"user_id"}, Locality: "us-east"},
}

recommendation := hatSql.RecommendSQLArrangement(candidates, hatSql.SQLArrangementWorkload{
	FilterFields:  []string{"user_id"},
	LocalityHints: []string{"us-east"},
})
// recommendation.Key == "user_id:east"
```

Hints and metadata are trimmed and compared case-insensitively. The selector
examines at most 32 hints and 128 bytes per hint or locality label. A locality
match adds an advisory score and appears as `LOCALITY` in the recommendation
reason. Without hints, the existing scoring and tie-breaking path is unchanged.

Locality alone cannot create a recommendation: at least one existing query
shape field (`WHERE`, `GROUP BY`, `ORDER BY`, or join field) must match. This
prevents a placement hint from selecting an otherwise irrelevant arrangement.

## Benchmark

Run with:

```text
make benchmark-mz035-arrangement-locality
```

Five 100 ms samples were collected on the local AMD Ryzen 9 5950X. The
benchmark compares two `user_id` arrangements and one filter field.

| Path | Samples (ns/op) | Median ns/op | B/op | Allocs/op |
| --- | --- | ---: | ---: | ---: |
| No locality hint | 536.3, 494.0, 464.9, 463.9, 417.7 | 464.9 | 56 | 3 |
| With locality hint | 498.0, 507.5, 505.3, 592.9, 524.5 | 507.5 | 80 | 4 |

The opt-in locality path was 1.09x the median CPU time, with one additional
allocation and 24 additional bytes per recommendation in this run. The
default path remains hint-free and retains its existing cost; the feature is
therefore useful for placement-aware planning only when the caller supplies
locality hints.
