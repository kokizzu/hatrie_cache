# CH-019 Materialized-View Storage Budget

`NewMaterializedViewsWithOptions` can bound the aggregate logical result held
by a materialized-view registry:

```go
views, err := hatSql.NewMaterializedViewsWithOptions(hatSql.MaterializedViewsOptions{
	MaxRows:  1_000_000,
	MaxBytes: 512 << 20,
})
if err != nil {
	return err
}
```

Both limits are optional. A zero limit is unlimited, and
`NewMaterializedViews()` keeps the prior unlimited behavior. The limits apply
across all views in the registry, not separately to each view. A create or
refresh that would exceed a limit returns `ErrMaterializedViewBudgetExceeded`
before publication, leaving the previous snapshots and usage unchanged.

`Usage()` reports retained logical rows and bytes plus the configured limits.
Logical bytes are the encoded size of retained result rows, not a claim about
Go heap size. Byte accounting is performed only when `MaxBytes` is configured;
row-only budgets avoid that serialization work.

This is a ClickHouse-inspired operational guard for projection growth. It does
not change SQL, wire, journal, snapshot, or persistence formats. It is opt-in,
and query execution still has its existing transient result limits; the budget
controls what the registry retains after a successful query.

## Verification

```text
make test-ch019-materialized-view-budget
make race-ch019-materialized-view-budget
make vet-ch019-materialized-view-budget
make benchmark-ch019-materialized-view-budget
```

The bounded refresh benchmark adds a measurable admission cost because it
serializes result rows to calculate logical bytes. The safety bound is the
reason to enable `MaxBytes`; use only `MaxRows` when row cardinality is the
needed guard without byte accounting.
