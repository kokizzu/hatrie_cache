# T-U21 Versioned Migration Manager

`hatSchema.VersionedMigrationManager` is an opt-in control-plane coordinator
for reversible schema migrations. It complements the older callback-oriented
`SpaceMigrationManager` documented in [TU21_SPACE_MIGRATION.md](TU21_SPACE_MIGRATION.md):
this API owns the `Schema`/`Migration` state machine, while callers still own
row conversion, routing, and durable checkpoint publication.

## Guarantees

- `NewVersionedMigrationPlan` requires contiguous migration versions.
- Every step is previewed and reversed while the plan is built. A plan with a
  non-reversible `Down` change is rejected before it can run.
- `ApplyNext` evaluates preconditions against a private schema clone and
  publishes one step with a generation check. A stale concurrent operation
  cannot publish over a newer state.
- `AdmitClient` tracks the schema version used by each client. During an active
  migration, versions from the base through the current version are allowed.
  A client on the base version does not block rollback; any client on a newer
  version does.
- `Rollback` replays reverse changes on a clone, verifies the base fingerprint,
  and only then publishes the rolled-back state.
- Checkpoints contain the plan ID, progress, phase, client versions, and schema
  fingerprint. The JSON payload is wrapped with SHA-256 and bounded before
  decoding.

The manager is not connected to SQL routing by default. An application must
explicitly gate requests through `AdmitClient`, call `ApplyNext`, publish
checkpoints, and decide when clients can be released.

## Example

```go
import (
	"context"

	"hatrie_cache/hat/hatSchema"
)

base := hatSchema.Schema{
	Version: 0,
	Sources: map[string]hatSchema.Source{
		"users": {
			Name:    "users",
			Columns: []hatSchema.Column{{Name: "id", Type: hatSchema.TypeInteger, NotNull: true}},
		},
	},
}

plan, err := hatSchema.NewVersionedMigrationPlan("users-v2", base, []hatSchema.VersionedMigrationStep{
	{
		Migration: hatSchema.Migration{
			Version: 1,
			Name:    "add-email",
			Up: []hatSchema.Change{{
				Kind:       hatSchema.ChangeAddColumn,
				SourceName: "users",
				Column:     hatSchema.Column{Name: "email", Type: hatSchema.TypeText},
			}},
			Down: []hatSchema.Change{{
				Kind:       hatSchema.ChangeDropColumn,
				SourceName: "users",
				Column:     hatSchema.Column{Name: "email", Type: hatSchema.TypeText},
			}},
		},
		Preconditions: []hatSchema.VersionedMigrationPrecondition{
			func(schema hatSchema.Schema) error {
				return nil // Check external quiescence or dependency state here.
			},
		},
	},
})
if err != nil {
	return err
}

manager, err := hatSchema.NewVersionedMigrationManager(plan)
if err != nil {
	return err
}
if err := manager.AdmitClient("api-reader", 0); err != nil {
	return err
}
if err := manager.ApplyNext(context.Background()); err != nil {
	return err
}

checkpoint := manager.Snapshot()
encoded, err := hatSchema.MarshalVersionedMigrationCheckpoint(checkpoint)
if err != nil {
	return err
}
// Persist encoded atomically using the service's backup/checkpoint store.
_ = encoded
```

`Restore` reconstructs the schema by replaying the plan's migrations and
rejects a mismatched version, fingerprint, plan ID, client version, phase, or
checksum:

```go
checkpoint, err := hatSchema.UnmarshalVersionedMigrationCheckpoint(encoded)
if err != nil {
	return err
}
if err := manager.Restore(checkpoint); err != nil {
	return err
}
```

## Recovery Rules

1. Load the same named plan and its code-defined preconditions.
2. Decode the checkpoint with `UnmarshalVersionedMigrationCheckpoint`.
3. Call `Restore`; do not trust a checkpoint's schema bytes as the source of
   truth. The manager reconstructs the schema from the base and migrations.
4. Resume with `ApplyNext` or release post-base clients and call `Rollback`.
5. Publish a new checkpoint after every successful state transition.

The checksum detects accidental corruption and untrusted edits; it is not
encryption or authentication. Store checkpoints with the same access control
as the database and use authenticated storage when an attacker can write the
checkpoint path.

## Limits And Cost

The default bounds are 1,024 steps per plan, 4,096 admitted clients, and a
1 MiB encoded checkpoint. The implementation is deliberately default-off and
does no work unless a caller constructs the manager.

Five-run medians on the repository benchmark host (AMD Ryzen 9 5950X,
linux/amd64) were:

| Operation | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Existing `Preview` + `Fingerprint` baseline | 879.8 | 1,072 | 15 |
| New manager construction + one `ApplyNext` | 7,513 | 12,672 | 83 |
| Checkpoint marshal | 2,051 | 690 | 5 |
| Checkpoint unmarshal | 7,981 | 2,608 | 37 |
| Manager construction + one client admission | 6,390 | 10,496 | 75 |

The apply comparison includes plan validation and manager construction in each
iteration, so it is not a claim that schema application became faster. The
feature adds bounded recovery and compatibility checks at a control-plane cost;
do not put `Snapshot`, JSON encoding, or client admission in a row hot path.

Reproduce the measurements and verification with:

```text
make benchmark-tu21-versioned
make test-tu21-versioned
make race-tu21-versioned
make vet-tu21-versioned
```

## Relationship To The Older Coordinator

Use [TU21_SPACE_MIGRATION.md](TU21_SPACE_MIGRATION.md) when the application
needs a generic callback lifecycle with pause/resume and callback-owned work.
Use this document's `VersionedMigrationManager` when the application has
concrete reversible `hatSchema.Migration` steps and needs a schema fingerprint
checked during recovery. They are independent and neither changes default SQL
or routing behavior.
