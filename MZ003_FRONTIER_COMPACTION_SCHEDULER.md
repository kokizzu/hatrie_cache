# Frontier-Aware Compaction Scheduler

`hatPipeline.FrontierCompactionScheduler` is an opt-in admission layer for
maintenance jobs that remove history. It combines the existing bounded
`Scheduler` with `FrontierRetentionRegistry` so a compaction callback cannot be
queued until its requested removal boundary is currently safe.

## Safety Rule

For a request `(frontierID, boundary)`, history strictly before `boundary` is
admitted only when:

- the frontier lower bound has reached at least `boundary`; and
- no active historical-read lease requires a timestamp below `boundary`.

The boundary is inclusive as an admission limit: `boundary <= safe` is allowed.
The compactor still owns the actual rewrite/delete operation and must use the
same boundary that it submitted.

## Usage

```go
retention, err := hatPipeline.NewFrontierRetentionRegistry(frontiers, hatPipeline.FrontierRetentionOptions{})
if err != nil {
	return err
}
compactions, err := hatPipeline.NewFrontierCompactionScheduler(ctx, retention, 2, 16)
if err != nil {
	return err
}
defer compactions.Close()

err = compactions.Submit(ctx, "orders", safeBoundary, func(ctx context.Context) error {
	return compactOrders(ctx, safeBoundary)
})
```

`Submit` waits on the caller's context until the frontier and leases allow the
boundary, then submits the callback to the bounded worker queue. A blocked
submitter does not occupy a worker. `Cancel` also unblocks frontier waits;
`Close` stops new submissions and lets already queued work drain; `Wait`
returns the first callback error.

For callers that do not need workers, `FrontierRetentionRegistry.WaitUntilSafe`
provides the same cancellable admission check without creating a scheduler.

## Defaults And Limits

No existing compaction path is changed and no background goroutine is started
unless the caller creates this scheduler. Worker and queue sizing use the same
rules as `NewScheduler`; the zero queue capacity uses direct handoff.

This is an in-process control-plane primitive. It does not persist pending
jobs, choose priorities, coalesce requests, or make an arbitrary compactor's
rewrite atomic with frontier publication. Those remain caller responsibilities.

## Verification

Focused correctness, package, race, vet, and benchmark targets are:

```text
make test-mz003-c227
make test-mz003-package-c227
make race-mz003-c227
make vet-mz003-c227
make benchmark-mz003-c227
```

The tests cover frontier lag, active read-lease blocking, cancellation,
missing frontiers, and callback execution after admission.
