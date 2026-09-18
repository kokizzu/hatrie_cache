# TR-013 Compaction Debt Scheduler

The persistent store already supports explicit compaction, but callers had to
choose when to invoke it. This feature adds an opt-in debt counter that turns
estimated rewrite volume into a compaction decision. It is inspired by
Tarantool/Vinyl-style background rewrite pressure: small writes accumulate
debt, while a larger batch is compacted once the configured threshold is
reached.

The scheduler is deliberately not enabled by construction. It creates no
goroutine and performs no storage work until the caller invokes
`CompactIfDue` or `Run`.

## API

```go
scheduler, err := NewCompactionDebtScheduler(CompactionDebtSchedulerOptions{
	ThresholdBytes: 64 << 20,
	CheckInterval:  time.Second,
	CompactOptions: LevelDBCompactionOptions{},
	Compact: func(options LevelDBCompactionOptions) (LevelDBCompactionResult, error) {
		return store.Compact(options)
	},
})
if err != nil {
	return err
}

// Call after a successful write using the estimated bytes that add rewrite
// pressure. AddDebt only records state; it does not start I/O.
if err := scheduler.AddDebt(estimatedBytes); err != nil {
	return err
}

// A service may own this goroutine, or call CompactIfDue from its maintenance
// loop instead. Cancellation stops Run without running another compaction.
go scheduler.Run(ctx)
```

Zero-valued `ThresholdBytes` and `CheckInterval` use the sane defaults of
64 MiB and one second. Negative values are rejected. The scheduler uses
atomic debt and running-state bookkeeping, so writers can add debt while a
compaction callback runs. Debt added during that callback remains for the next
cycle. A failed callback retains its claimed debt for retry and returns an
error wrapped with `ErrCompactionDebtSchedulerCompaction`.

The callback owns the actual storage operation and its error policy. The
scheduler does not guess write sizes, inspect the database, or silently start
background work. This keeps the feature usable with both LevelDB and Pebble
compaction adapters, and makes the default behavior identical to the existing
manual-compaction path.

## Tradeoff

Debt scheduling reduces repeated compaction callbacks and can smooth rewrite
amplification, but it intentionally delays reclamation until the threshold is
reached. Between cycles, obsolete files or read amplification may remain
higher. Choose a smaller threshold for tighter space recovery and a larger
threshold for fewer maintenance calls. The feature should remain off when an
operator already owns compaction cadence or when immediate reclamation is more
important than batching.

## Verification

Focused correctness tests cover default validation, threshold gating,
concurrent debt, debt arriving during compaction, failed-compaction retry, and
context cancellation. The focused race test and package vet pass through the
TR-013 Makefile targets.

The benchmark is a zero-allocation control-plane test with a no-op compaction
callback. It measures scheduler overhead and callback suppression, not disk
throughput. Five `-benchmem` samples ran on Linux/amd64 with an AMD Ryzen 9
5950X after the atomic bookkeeping refinement:

| Path | Median ns/op | Median B/op | Median allocs/op | Compactions/op | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| Immediate callback control | 1.799 | 0 | 0 | 1.00000 | baseline |
| Debt scheduler | 10.07 | 0 | 0 | 0.01562 | 64x fewer callbacks; 5.60x control CPU |
| Below-threshold debt accounting | 2.217 | 0 | 0 | 0 | no compaction |

The initial mutex implementation measured about 18.5 ns/op for the scheduled
path and about 5.3 ns/op for below-threshold accounting. Replacing that
bookkeeping with atomics improved those paths by about 1.84x and 2.39x while
preserving the zero-allocation result. The callback count is the main expected
benefit; the absolute nanoseconds are not representative of a real storage
compaction.

Run the checks with:

```sh
make test-tr013-compaction-debt
make race-tr013-compaction-debt
make vet-tr013-compaction-debt
make benchmark-tr013-compaction-debt
```
