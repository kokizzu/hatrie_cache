# CH-G34 Mutation Priority And Kill Control

`hatSql.MutationController` is an opt-in executor for online backfills,
rebuilds, and other explicit mutations that need bounded admission and
operator control. Existing SQL execution is unchanged unless a caller creates
and submits work to a controller.

## API

```go
controller, err := hatSql.NewMutationController(hatSql.MutationControllerOptions{
	QueueCapacity:   64,
	Workers:         1,
	HistoryCapacity: 128,
})
if err != nil {
	return err
}
defer controller.Close()

handle, err := controller.Submit(ctx, hatSql.MutationSpec{
	ID:       "orders-backfill-2026-09",
	Priority: 10,
	Run: func(ctx context.Context, report hatSql.MutationProgressReporter) error {
		for completed := uint64(0); completed < total; completed++ {
			if err := doOne(ctx); err != nil {
				return err
			}
			if err := report(hatSql.MutationProgress{
				Completed: completed + 1,
				Total:     total,
			}); err != nil {
				return err
			}
		}
		return nil
	},
})
if err != nil {
	return err
}

// An operator or request handler can stop queued/running work by ID.
_ = controller.Cancel(handle.ID())
return handle.Wait(ctx)
```

`MutationHandle.Snapshot`, `MutationController.Snapshot`, `Snapshots`, and
`Stats` expose progress and bounded terminal history. `Wait` returns the
producer error, `context.Canceled` for cancellation, or the caller's wait
context error if the wait itself times out.

## Scheduling Semantics

- Higher `Priority` values run first.
- Equal priorities preserve submission order.
- Queue capacity bounds waiting mutations; currently running work is not
  counted against that queue bound.
- Duplicate IDs are rejected while active and while retained in history.
- Queued cancellation removes work immediately. Running cancellation calls the
  mutation context and completes when the callback returns.
- Progress must be monotonic. A non-zero total cannot change, and completed
  work cannot exceed it.
- A callback must honor its context. `Close` cancels all work and waits for
  callbacks to return; Go cannot forcibly stop a callback that ignores context.
- Panics in callbacks become failed mutations rather than killing a worker.
- Error text retained in snapshots is capped at 4 KiB.

## Defaults And Bounds

| Option | Default | Maximum |
| --- | ---: | ---: |
| `QueueCapacity` | 64 | 4096 |
| `Workers` | 1 | 64 |
| `HistoryCapacity` | 128 | 4096 |

Zero or negative option values use defaults. Values above the maximum are
rejected. The conservative default of one worker protects foreground traffic
and makes ordering predictable; callers can explicitly increase workers after
measuring their workload.

## Cost Measurement

Command: `make bench-chg34-mutation-control`

Five 100 ms samples on AMD Ryzen 9 5950X, Go amd64:

| Path | Median | Memory | Allocations |
| --- | ---: | ---: | ---: |
| Submit, queue, run, report, wait | ~2.4 us/op | 440 B/op | 6 allocs/op |
| Direct callback baseline | ~1.75 ns/op | 0 B/op | 0 allocs/op |

The controller is not intended to replace a direct function call on a hot
single-item path. Its cost buys priority ordering, bounded admission,
cancellation, progress, and retained status. Because it is opt-in and has no
default SQL integration, unused applications pay no controller cost.

## Verification

- `make test-chg34-mutation-control`
- `make race-chg34-mutation-control`
- `make vet-chg34-mutation-control`
- `make bench-chg34-mutation-control`

The focused tests cover priority ordering, full-queue rejection, cancellation,
progress validation, terminal snapshots, and shutdown. A full `make
test-chg34-mutation-control-package` run currently also reaches unrelated
typed-table aggregate checkpoint tests in the concurrent worktree; those
failures are outside this controller and are reported separately.
