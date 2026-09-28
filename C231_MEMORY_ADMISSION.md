# C231 Memory Admission

`hatWorkload.MemoryAdmissionController` adds an optional per-workload-class
memory budget to the existing concurrency admission controller. It is useful
for query groups, background jobs, and sinks that need a bounded estimate of
in-flight memory in addition to a maximum number of concurrent operations.

## Usage

```go
controller, err := hatWorkload.NewMemoryAdmissionController(
	hatWorkload.AdmissionOptions{MaxInFlight: 64, MaxQueued: 1024},
)
if err != nil {
	return err
}
if err := controller.RegisterClass(hatWorkload.MemoryClassOptions{
	ClassOptions: hatWorkload.ClassOptions{
		Name:        "interactive",
		Priority:    10,
		MaxInFlight: 16,
	},
	MaxMemoryBytes: 256 << 20,
}); err != nil {
	return err
}

lease, err := controller.Acquire(ctx, "interactive", estimatedBytes)
if err != nil {
	return err
}
defer lease.Release()
```

`MaxMemoryBytes == 0` means unlimited memory for that class. A request larger
than a non-zero class budget returns `ErrMemoryAdmissionRequestTooLarge`.
Reservations are bounded by `AdmissionOptions.MaxQueued`; waiting requests
honor context cancellation. A reservation is held while the request waits for
the underlying concurrency slot, so the reported budget cannot be exceeded by
queued work. `MemoryAdmissionLease.Release` is idempotent.

`Snapshot()` returns the existing admission report under `Admission`, plus
controller totals and per-class `MaxMemoryBytes`, `ReservedMemoryBytes`, and
`ActiveMemoryBytes`. Reserved bytes include memory granted to a request that is
still waiting for a concurrency slot; active bytes have both reservations.

The feature is opt-in. Existing users of `AdmissionController` and all default
workload behavior remain unchanged.

## Verification

The red test was run against the previous origin commit before implementation.
The final package test and race commands are exposed through:

```text
make test-c231-memory-admission
make race-c231-memory-admission
```

Both run the complete `hat/hatWorkload` package. Temporary benchmark worktrees
are removed by their shell-script traps.

## Benchmark

The benchmark uses one registered class, one concurrency slot, a 64-byte
request, and `-benchmem -count=5` on the same AMD Ryzen 9 5950X host.

| Path | Samples (ns/op) | B/op | allocs/op | Relative latency |
| --- | --- | ---: | ---: | ---: |
| Existing `AdmissionController` | 63.11, 68.39, 77.12, 70.62, 62.69 | 4 | 1 | 1.00x |
| `MemoryAdmissionController` | 156.2, 166.5, 156.2, 156.2, 169.5 | 8 | 2 | 2.35x |

The memory-aware path therefore costs about 2.35x latency, 2.00x allocated
bytes, and 2.00x allocations for this uncontended acquire/release workload.
That is an opt-in feature cost, not a regression to the existing controller.

The first implementation measured 327.3, 290.8, 276.0, 309.0, and 294.9
ns/op with 168 B/op and 4 allocations. Removing the immediate-grant waiter
allocation reduced the final path to the measurements above, about 1.99x
lower latency, 21x lower allocated bytes, and 2x fewer allocations than that
initial implementation.
