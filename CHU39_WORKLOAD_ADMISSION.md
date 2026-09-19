# CH-U39 Workload Admission Priorities

`hatSql.SQLWorkloadAdmission` is an opt-in, bounded admission controller for
SQL handlers and other application-owned work. It adds caller-defined workload
classes, priorities, weighted sharing, cancellation, and a starvation bound.

The feature is off by default. Existing SQL execution does not create or
consult a controller. Applications opt in by constructing one and wrapping the
handler that should be limited.

## Example

```go
admission, err := hatSql.NewSQLWorkloadAdmission(hatSql.SQLWorkloadAdmissionOptions{
	MaxConcurrent:    8,
	MaxPending:       1024,
	MaxPriorityBurst: 8,
	Classes: []hatSql.SQLWorkloadClass{
		{Name: "interactive", Priority: 10, Weight: 3},
		{Name: "batch", Priority: 1, Weight: 1},
	},
})
if err != nil {
	return err
}
defer admission.Close()

err = admission.Run(ctx, "interactive", func(ctx context.Context) error {
	_, err := hatSql.ExecuteSQLQueryContext(ctx, source, resolver, options)
	return err
})
```

`Run` releases the permit after a normal return, an error, or a panic. A panic
is still propagated to the caller. `Acquire` is useful when work spans more
than one function; its returned release function is safe to call more than
once.

## Configuration

| Option | Meaning | Zero value |
| --- | --- | --- |
| `MaxConcurrent` | Active permits across all classes. | `1` |
| `MaxPending` | Total queued requests, excluding active work. | `1024` |
| `MaxPriorityBurst` | Consecutive selections for the highest queued priority before the highest lower-priority class gets a turn. | `8` |
| `Classes` | Allowed class definitions. | An implicit `default` class |

Class names are trimmed and lowercased. Unknown names and duplicate configured
names are rejected. Higher `Priority` values are selected first. `Weight`
controls the service share among queued classes at the same priority; zero
means weight one. The controller has no background goroutine. Queue dispatch
happens when a permit is released, which keeps shutdown deterministic.

When the pending queue is full, callers wait for capacity or return their
context error. `Close` rejects new work and wakes all queued callers with
`ErrSQLWorkloadAdmissionClosed`; active work remains responsible for releasing
its permit.

## Operational Guidance

- Create one controller per process, service, or intentionally isolated tenant,
  rather than one per query.
- Use a bounded `MaxPending` when request contexts are tied to clients so
  disconnected clients leave the queue promptly.
- Keep class names and priorities configuration-owned. Unknown class names are
  rejected instead of creating unbounded per-request state.
- Call `Close` during graceful shutdown, then allow active handlers to finish.
- `Stats` reports active and pending counts, admissions, cancellations, and the
  configured bounds for monitoring.

## Benchmark

The benchmark uses a 64-value integer-sum callback on an AMD Ryzen 9 5950X,
Go's standard benchmark runner, five samples per case, and reports the median
of those samples. The direct case is the same callback without admission. The
acquire case measures the internal uncontended permit path; the run case
measures the public wrapper including callback invocation and panic-safe
release.

| Case | Raw samples (ns/op) | Median | Memory |
| --- | --- | ---: | --- |
| Direct callback | 22.94, 22.73, 22.06, 22.79, 21.96 | 22.73 | 0 B/op, 0 allocs/op |
| Uncontended acquire/release | 24.36, 24.21, 24.66, 24.83, 24.81 | 24.66 | 0 B/op, 0 allocs/op |
| `Run` | 55.01, 51.84, 50.89, 52.73, 54.18 | 52.73 | 0 B/op, 0 allocs/op |

Before notification-path optimization, the same acquire path measured a
median `102.8 ns/op`, `112 B/op`, and one allocation; `Run` measured a median
`142.5 ns/op`, `112 B/op`, and one allocation. The optimized final path is
therefore about 4.2x faster for acquire and 2.7x faster for `Run`, while
removing the per-operation allocation. The final deferred release is retained
because it guarantees permit recovery after a callback panic.

Reproduce with:

```text
make benchmark-chu39
```
