# Differential Late-Data Policy

`ApplyDifferentialLateDataPolicy` makes late-data handling explicit for
`DifferentialRow` batches. A row is late when `row.Time < frontier`; a row at
the frontier is on time.

The available policies are:

| Policy | Behavior |
| --- | --- |
| `DifferentialLateDataAccept` | Retain every row in input order. |
| `DifferentialLateDataDrop` | Remove only rows older than the frontier. |
| `DifferentialLateDataReject` | Return an error if any row is late. |

The function does not advance the frontier, mutate input rows, or implicitly
choose a policy for existing callers. Returned rows own cloned row maps. A
reject result is `nil` and contains no partial output; classify it with
`errors.Is(err, hatSql.ErrDifferentialLateDataRejected)`. Invalid policy values
return `hatSql.ErrDifferentialLateDataPolicyInvalid`.

```go
onTime, err := hatSql.ApplyDifferentialLateDataPolicy(
	updates,
	watermark,
	hatSql.DifferentialLateDataReject,
)
if err != nil {
	if errors.Is(err, hatSql.ErrDifferentialLateDataRejected) {
		// Route the batch to a late-data workflow or retry policy.
	}
}
```

Use `Accept` when downstream differential operators can incorporate late
updates, `Drop` only when loss is intentional, and `Reject` when the caller
must explicitly route or repair late input.

## Measured Cost

Benchmark command:

```text
make benchmark-sql-differential-late-data
```

For 1,024 rows with a frontier that drops half of them, the development
machine measured:

| Metric | Result |
| --- | ---: |
| Time | 96-99 us/op |
| Allocated memory | 212,993 B/op |
| Allocations | 1,025 allocs/op |

The cost includes the returned slice and cloned row maps. Reject performs a
validation pass first and returns without allocating partial output.
