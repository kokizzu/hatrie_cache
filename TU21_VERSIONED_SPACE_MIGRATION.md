# Versioned Space Migration Manager

T-U21 adds an importable `hatSchema.SpaceMigrationManager` for named schema
rollouts. It is a control-plane state machine, not a hidden background worker.
The caller owns row conversion, locking, durable publication, and the storage
used to persist snapshots.

## Safety Contract

`SpaceMigrationPlan` requires an explicit plan version, stable plan and space
names, a base schema version, and the exact base fingerprint. Migrations must
be contiguous and bounded by `MaxSpaceMigrationSteps`.

The default `SpaceMigrationCompatibilityRolling` mode runs the existing
`CheckRollingCompatibility` preflight for every step. It accepts only changes
that old and new clients can read together, such as nullable appended columns.
`SpaceMigrationCompatibilityExclusive` is an explicit opt-in for a caller that
has already fenced old clients.

The manager also previews every `Down` change before starting. A plan whose
reverse path does not restore the base fingerprint is rejected before any
callback runs.

## Lifecycle

```go
plan := hatSchema.SpaceMigrationPlan{
	PlanVersion:     hatSchema.SpaceMigrationPlanVersion,
	ID:              "users-v3",
	Space:           "users",
	BaseVersion:     schema.Version,
	BaseFingerprint: schema.Fingerprint(),
	Compatibility:   hatSchema.SpaceMigrationCompatibilityRolling,
	Migrations: []hatSchema.Migration{
		{
			Version: 2,
			Name:    "add email",
			Up: []hatSchema.Change{{
				Kind:       hatSchema.ChangeAddColumn,
				SourceName: "users",
				Column:     hatSchema.Column{Name: "email", Type: hatSchema.TypeText},
			}},
			Down: []hatSchema.Change{{
				Kind:       hatSchema.ChangeDropColumn,
				SourceName: "users",
				Column:     hatSchema.Column{Name: "email"},
			}},
		},
	},
}

manager := hatSchema.NewSpaceMigrationManager()
_ = manager.RegisterPlan(plan)
_ = manager.Start(plan.ID, schema)

err := manager.Apply(ctx, plan.ID, func(ctx context.Context, op hatSchema.SpaceMigrationOperation) error {
	// Convert rows and publish the step through the caller's storage boundary.
	return convertAndPublish(ctx, op.Space, op.From, op.To)
})
```

`Progress` reports the current version, next zero-based step, terminal status,
and the last error. A failed callback leaves earlier committed steps intact;
calling `Apply` again resumes from `NextStep`. `Rollback` invokes the callback
in reverse order and publishes each schema only after the callback succeeds.

## Recovery

`Snapshot()` returns a deterministic, clone-safe `SpaceMigrationSnapshot` with
plans, base schemas, current schemas, status, and progress. Persist that value
with the application's existing atomic backup mechanism and pass it to
`Restore()` during startup. A snapshot captured while a callback is in flight
is recovered as `Failed`, so the caller can retry an idempotent callback rather
than assuming an uncertain commit.

The manager does not write files, acquire distributed locks, route clients, or
claim exactly-once row conversion. Those responsibilities remain deployment
and storage policy, which keeps the default path explicit and auditable.

## Cost Profile

The manager is a schema-change control-plane tool, not a per-row hot path. On
Linux/amd64 with an AMD Ryzen 9 5950X, five runs at `-benchtime=200ms` measured
the two-step example as follows:

| Workload | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Direct `Preview` sequence baseline | 1,662 | 2,160 | 8 |
| Manager `Apply` with no-op callbacks | 7,567 | 10,992 | 74 |
| Manager `Snapshot` | 1,922 | 3,344 | 13 |

The approximately 4.6x apply overhead buys preconditions, lifecycle state,
callback isolation, resumability, and rollback tracking. It is acceptable for
infrequent migrations and deliberately unsuitable for row-by-row execution.
Raw samples are recorded in [BENCHMARK.md](BENCHMARK.md#t-u21-versioned-space-migration-manager).
