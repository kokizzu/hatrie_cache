# M-U36 Hydration Progress And Admission

Status: implemented as an opt-in `hatSql.TypedTableArrangementHydrationAdmission` contract.

## Purpose

Materialize-style arrangements can be present but stale after restart or a
checkpoint restore. This API gives callers a bounded progress registry and an
admission gate so a query does not observe a partially hydrated arrangement.
It does not start background work or change existing arrangement behavior.

## Usage

```go
admission, err := hatSql.NewTypedTableArrangementHydrationAdmission(1024)
if err != nil {
    return err
}

report, err := admission.HydrateAggregate(ctx, "orders_by_customer", arrangement, 1024)
if err != nil {
    return err
}

if err := admission.Admit(ctx, "orders_by_customer"); err != nil {
    return err
}
_ = report
// Execute the dependent query after admission succeeds.
```

`HydrateJoin` and `RegisterJoin`/`UpdateJoin` track both inputs of a join.
`Admit(ctx, key...)` waits for every requested key, returns context
cancellation, and propagates a terminal hydration error. `Register` is the
explicit recovery boundary after failure and may reset a checkpoint.

Snapshots are sorted by key and detached from the registry. The entry count
and key size are bounded; waiters use notifications rather than polling or one
goroutine per waiter. The registry is not durable, so the caller owns
checkpoint persistence, scheduling, and restart reconciliation.

## Benchmark

Five runs of `make benchmark-mu36` on an AMD Ryzen 9 5950X, Go benchmark mode:

| Path | Before fast path | After fast path | Memory after | Allocations after |
| --- | ---: | ---: | ---: | ---: |
| Direct arrangement freshness baseline | 12.19 ns/op | 13.28 ns/op | 0 B/op | 0 allocs/op |
| Ready single-key admission | 65.51 ns/op, 16 B/op | 16.54 ns/op | 0 B/op | 0 allocs/op |
| One-entry progress snapshot | 128.1 ns/op, 136 B/op | 128.4 ns/op | 136 B/op | 2 allocs/op |

The ready admission fast path is about 3.96x faster and removes its one
allocation. It is still about 1.31x the direct freshness check, so admission
remains opt-in. A snapshot is intentionally a detached reporting operation and
retains its small copy cost.

## Verification

- `make test-mu36-red` passes the focused behavior and allocation contract.
- `make race-mu36` passes.
- `make vet-mu36` passes.
- `make test-mu36-package` reaches unrelated existing M-U05 checkpoint failures; no M-U36 test fails. The failures are in `m_u05_arrangement_recovery_global_test.go` and `m_u05_arrangement_recovery_test.go`.
