# Catalog Migration Runner

M-U49 adds a small, dependency-aware runner for SQL catalog changes. It is a
control-plane API: the caller still owns the actual SQL/catalog operations,
locks, transactions, and durable migration history.

## Plan And Dry Run

Build a `hatSql.CatalogMigrationPlan` with the current
`hatSql.CatalogMigrationPlanVersion`, then call `OrderedSteps()` before any
side effect. The method validates the version, step count, non-empty unique
step IDs, object/action fields, dependency IDs, duplicate dependencies, and
cycles. It returns a deterministic topological order: ready steps are ordered
by ID. This makes a dry run suitable for review, logging, or a migration
preview endpoint.

```go
plan := hatSql.CatalogMigrationPlan{
	Version: hatSql.CatalogMigrationPlanVersion,
	Steps: []hatSql.CatalogMigrationStep{
		{ID: "create-users", Object: "users", Action: "create"},
		{ID: "create-email-index", Object: "users_email", Action: "create", DependsOn: []string{"create-users"}},
	},
}
ordered, err := plan.OrderedSteps()
```

`OrderedSteps` does not execute SQL and does not claim that a migration is
safe for a particular database. The caller must validate database-specific
syntax and compatibility separately.

## Apply And Rollback

Provide `ApplyStep` and `RollbackStep` callbacks and call `Apply(ctx, plan)`.
Steps run serially in the validated order. On the first apply error, already
applied steps are rolled back in reverse order. Rollback receives a context
with cancellation removed so a client request cancellation does not skip
cleanup. Rollback errors are retained in `CatalogMigrationError.RollbackErrs`
and in the returned result; the original apply error remains available through
`errors.Is`/`errors.As`.

The runner does not automatically retry DDL, acquire distributed locks, write
history rows, or hide partial failure. Those policies depend on the SQL engine
and must be implemented by the callback owner. A deployment should persist the
plan ID/version and result before advertising the new catalog to mixed-version
clients.

Plans are bounded by `hatSql.MaxCatalogMigrationSteps` (currently 4,096) to
keep validation and rollback state bounded. The plan version is intentionally
explicit so serialized plans can be rejected rather than silently interpreted
under a different contract.

## Benchmark

Command:

```sh
make benchmark-m049
```

Five runs on Linux/amd64, AMD Ryzen 9 5950X:

| Workload | ns/op samples | Median ns/op | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| Three-step loop control | 12.48, 12.52, 12.27, 12.36, 12.33 | 12.36 | 0 | 0 |
| Validate, order, apply three steps | 1592, 1607, 1685, 1713, 1586 | 1607 | 848 | 12 |

The loop is a lower-bound control, not an old implementation of catalog
migrations. The measured cost is therefore the runner's validation, graph
ordering, result tracking, and callback orchestration. It is appropriate for
infrequent schema/catalog changes, not per-row query execution. See the raw
benchmark table in [BENCHMARK.md](BENCHMARK.md#m-u49-catalog-migration-runner).
