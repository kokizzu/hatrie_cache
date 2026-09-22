# M242 Per-Operator Update, Batch, And Frontier Metrics

M242 adds opt-in operator metrics to SQL observer events. Set
`SQLQueryOptions.OperatorMetrics = true` when the monitoring consumer needs
the additional sidecar:

```go
frontier := uint64(42)
var event hatSql.SQLQueryEvent
_, err := hatSql.ExecuteSQLQueryContext(ctx, query, resolver, hatSql.SQLQueryOptions{
	Observer: hatSql.SQLQueryObserverFunc(func(observed hatSql.SQLQueryEvent) {
		event = observed
	}),
	OperatorMetrics: true,
	AsOfFrontier:    &frontier,
})
```

The existing `QueryEvent.Operators` payload is unchanged. Metrics are returned
in the optional `QueryEvent.OperatorMetrics` sidecar in the same order as the
operator entries, with an explicit `Node` for safe correlation.

```json
{
  "operators": [{"node": "SCAN", "input_rows": 0, "output_rows": 2}],
  "operator_metrics": [{
    "node": "SCAN",
    "update_count": 2,
    "batch_count": 1,
    "frontier": 42
  }]
}
```

`UpdateCount` is the number of rows emitted by that recorded operator step.
`BatchCount` is one for each recorded execution step; repeated steps with the
same node remain separate so consumers can aggregate them without losing
batch boundaries. `Frontier` is present only when the query selected an exact
`AsOfFrontier`; live queries omit it because no exact logical frontier was
available to the SQL observer.

The option is default-off. Without it, no sidecar slice is allocated and the
legacy operator struct layout remains unchanged. Slow-query samples preserve
and deep-copy the sidecar when it is enabled. The event contains only operator
names and numeric counters; it does not add SQL text, predicates, keys, or row
values.

## Measurement

Command: `make benchmark-m242` on Linux amd64, AMD Ryzen 9 5950X, five
one-second samples per benchmark. CPU is noisy for this short query, so no
speedup claim is made; allocation and wire-size changes are the acceptance
criteria.

| Workload | Median ns/op | B/op | Allocs/op | Wire bytes |
| --- | ---: | ---: | ---: | ---: |
| Pre-M242 observer | 12,407 | 8,657 | 101 | n/a |
| Current observer, metrics off | 16,853 | 8,657 | 101 | n/a |
| Current observer, metrics on | 14,403 | 8,739 | 102 | n/a |
| Current JSON sidecar marshal | 1,549 | 625 | 2 | 477-478 |

The accepted design therefore leaves the default allocation profile unchanged.
The opt-in path costs about 82 B/op and one allocation for the additional
sidecar. An earlier inline-field design enlarged every `QueryOperator` and
added about 50 B/op even when metrics were disabled; it was rejected and is
not part of M242.

Raw samples:

```text
pre-M242 observer:       14368 11837 12376 13479 12407 ns/op; 8649-8657 B/op; 101 allocs/op
current observer off:   14729 17309 16853 17587 16367 ns/op; 8650-8657 B/op; 101 allocs/op
current metrics on:     17746 14403 13874 13499 14991 ns/op; 8737-8739 B/op; 102 allocs/op
current metrics JSON:    1501  1409  1699  1603  1549 ns/op; 625 B/op; 2 allocs/op; 477-478 wire bytes
```
