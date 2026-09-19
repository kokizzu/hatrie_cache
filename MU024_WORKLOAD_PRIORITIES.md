# M-U24 Workload Classes And Priorities

This progress note records the opt-in workload admission controller inspired by
Materialize workload classes. The controller is transport-independent: callers
can use the same named classes for interactive queries, background work, or
sinks.

## Contract

Create an `hatWorkload.AdmissionController`, register named classes, and call
`Acquire` before starting work. The returned `AdmissionLease` must be released
when the work ends. Higher numeric priorities are admitted first, FIFO is used
within one priority, and `PriorityBurst` forces a lower-priority waiter to run
after a bounded number of higher-priority grants.

The controller is opt-in. Existing code has no admission overhead unless it
constructs and uses one.

## Bounds And Cancellation

`MaxInFlight`, `MaxQueued`, and `MaxClasses` bound controller state. A canceled
queued context is removed without consuming capacity. If cancellation races
with a grant, the granted lease is returned and must still be released.

`Snapshot` returns deterministic class-sorted active and queued counts. Lease
release is idempotent and safe for concurrent callers.

## Measurement

Five-run medians on the repository's Ryzen 9 5950X host:

| Operation | Median | Memory | Allocations | Comparison |
|---|---:|---:|---:|---|
| Direct atomic counter baseline | 1.85 ns/op | 0 B/op | 0 | baseline only; it does not provide admission |
| Uncontended `Acquire` + `Release` | 45.23 ns/op | 4 B/op | 1 | 22.9x the direct baseline, with bounded admission |
| Two-class `Snapshot` | 258.0 ns/op | 216 B/op | 4 | diagnostic path, not request admission |

The first implementation measured about 264 ns/op, 208 B/op, and 3
allocations for uncontended admission. The immediate fast path and compact
copy-safe release token reduced that path by about 5.8x, reduced retained bytes
by 52x, and reduced allocations from three to one. The controller remains
opt-in because admission necessarily costs more than an ungoverned operation.

Run the focused checks with:

```text
make test-mu24
make test-race-mu24
make vet-mu24
make benchmark-mu24
```
