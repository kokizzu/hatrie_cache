# TT-029 Before/After Trigger Lifecycle

`hatSql` now supports an opt-in pure `Before` callback in addition to the
existing transactional `Prepare` callback used for AFTER actions.

## Behavior

1. `SQLTriggerTransaction.Add` collects row events.
2. Matching BEFORE callbacks run in trigger order on cloned events.
3. A BEFORE callback may reject the event or return changed `Before`/`After`
   row values.
4. `Source`, `Operation`, and `Key` metadata must be returned unchanged.
5. The primary mutation callback receives the transformed events.
6. Matching AFTER callbacks prepare actions from those same transformed events.
7. Any BEFORE, primary, prepare, commit, or rollback error preserves the
   existing transaction rollback behavior.

BEFORE callbacks must be pure with respect to externally visible state. They
should validate or transform the cloned event only; side effects belong in the
existing AFTER action lifecycle.

## Go API

```go
registry := hatSql.NewSQLTriggerRegistry()
err := registry.Register(hatSql.SQLTrigger{
    Name:      "normalize_name",
    Source:    "people",
    Operation: "UPDATE",
    Order:     10,
    Before: func(ctx context.Context, event hatSql.SQLTriggerEvent) (hatSql.SQLTriggerEvent, error) {
        event.After["name"] = strings.TrimSpace(event.After["name"].(string))
        return event, nil
    },
})
```

For parsed DDL, use the timing-specific registration methods:

```go
err := registry.RegisterSQLBeforeTrigger(
    "CREATE TRIGGER normalize_name BEFORE UPDATE ON people FOR EACH ROW",
    normalize,
)
err = registry.RegisterSQLTrigger(
    "CREATE TRIGGER audit AFTER UPDATE ON people FOR EACH ROW",
    prepareAudit,
)
```

`RegisterDefinition` remains the AFTER registration method. Use
`RegisterBeforeDefinition` for a parsed BEFORE definition. A BEFORE callback
that changes event metadata is rejected before the primary mutation runs.

## Benchmark

The benchmark uses one UPDATE event and one registry transaction per iteration,
with five samples on the development machine. The pre-change baseline measured
the existing AFTER-only path before BEFORE support was added.

| Path | Median ns/op | B/op | allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Pre-change AFTER-only | 2,578 | 3,696 | 25 | 1.00x |
| Post-change AFTER-only | 2,551 | 3,712 | 25 | 0.99x |
| Post-change BEFORE transform | 2,932 | 4,384 | 29 | 1.14x |

Run with:

```text
make benchmark-tt029-before-trigger
```

The AFTER-only path retained the same allocation count and was within normal
sample variance in bytes/op (+16 B/op in this run). BEFORE adds four
allocations and 688 B/op because it clones and replaces the event for the
explicit transformation. That cost is opt-in and is paid only by registries
that register a BEFORE callback; existing AFTER-only behavior remains
transactional and compatible.

## Coverage

Focused tests cover transformation visibility to the primary and AFTER paths,
rejection before primary apply, metadata mutation rejection, callback
validation, DDL parsing, and DDL registration. Package tests, race detection,
and `go vet` are run by `make verify-tt029-before-trigger`.
