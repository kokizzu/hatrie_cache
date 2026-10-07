# T-U38 Conflict Introspection

T-U38 adds an opt-in, bounded conflict history for replication and application
code that already resolves conflicts through `hatReplication.ConflictPolicyRegistry`.
It records the conflicting versions, selected policy, and outcome while keeping
the application key out of the log.

## Usage

```go
log, err := hatReplication.NewConflictInspectionLog(
	hatReplication.DefaultConflictInspectionCapacity,
)
if err != nil {
	return err
}

winner, err := registry.ResolveAndRecord(
	log,
	"orders",
	"order-42",
	left,
	right,
)
if err != nil {
	return err
}
_ = winner

page := log.Read(cursor, 256)
cursor = page.NextSequence
```

`ResolveAndRecord` uses the same per-space policy as `Resolve`, including source
priority and reject mode. A rejected conflict is recorded with
`ConflictInspectionRejected` and the original `ErrConflictRejected` is returned.
Invalid versions fail before an event is recorded. Passing a nil log preserves
the ordinary `Resolve` behavior and does not require a key.

## Retention and privacy

`ConflictInspectionLog` is a fixed-capacity in-memory ring. New events overwrite
the oldest event after the configured capacity. Sequences start at one and are
monotonic until process-level `uint64` exhaustion. `Read(after, limit)` returns
ascending events, a continuation cursor, and `Truncated=true` when the caller's
cursor is older than the retained history. The page size is capped at
`MaxConflictInspectionPageSize` (1,024).

The default capacity constant is 4,096 and the constructor rejects capacities
above 65,536. `KeyDigest` is a SHA-256 hex digest; the raw key is never retained
or returned. Space names and conflict version metadata, including `NodeID`, are
retained, so callers should pseudonymize those fields when their identity is
sensitive. `Snapshot` returns copied event storage.

The log is intentionally not attached to the registry and is not persisted or
sent automatically. Callers can serialize the JSON-tagged `ConflictInspectionPage`
or `ConflictInspectionEvent` values and apply their own authenticated transport,
retention, and durable storage policy.

## Measured cost

Linux/amd64, AMD Ryzen 9 5950X, five `-benchmem` samples per path:

| Path | Raw ns/op samples | Median ns/op | Median B/op | Median allocs/op | Relative result |
| --- | --- | ---: | ---: | ---: | --- |
| Existing registry resolve | `16.98; 18.90; 19.93; 18.10; 20.29` | `18.90` | `0` | `0` | baseline |
| Opt-in `ResolveAndRecord` | `210.3; 206.2; 202.3; 206.6; 211.1` | `206.6` | `128` | `2` | `10.93x` latency, +128 B, +2 allocs |
| Read 64 retained events | `1639; 1865; 1793; 1612; 1692` | `1692` | `10880` | `1` | bounded page copy |

The default direct and registry resolution paths remain at zero allocations. The
recording overhead is therefore paid only by callers that explicitly install a
log and call `ResolveAndRecord`; this feature does not start a worker, change
replication defaults, or change storage and wire formats.

## Verification

`make benchmark-tu38-baseline` runs the existing conflict-resolution control;
`make benchmark-tu38` runs the control, recording, and bounded-read benchmarks.
`make test-tu38`, `make test-tu38-race`, and `make vet-tu38` rerun the focused
tests, race check, and package vet check through the repository Makefile.
