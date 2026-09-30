# M222 Replicated Compute Router

`hatPipeline.PlanDataflowOperatorPlacement` already produces deterministic
operator replicas across workers and failure domains. M222 adds the runtime
health and failover layer with `ReplicatedComputeRouter`.

```go
plan, err := hatPipeline.PlanDataflowOperatorPlacement(operators, workers, hatPipeline.DataflowPlacementOptions{})
if err != nil {
    return err
}
router, err := hatPipeline.NewReplicatedComputeRouter(plan)
if err != nil {
    return err
}

assignment, err := router.Select("orders-index")
if err != nil {
    return err
}
// Run the maintained-index work on assignment.WorkerID.
if err := runOnWorker(ctx, assignment); err != nil {
    // Report failure only after the caller has made its idempotency/side-effect
    // decision. The router intentionally does not retry the callback.
    _ = router.MarkWorkerUnhealthy(assignment.WorkerID)
}
```

Replica zero is preferred. When its worker is unhealthy, selection advances in
replica order; when all replicas are unhealthy,
`ErrReplicatedComputeRouterUnavailable` is returned. `MarkWorkerHealthy` restores
a worker for every operator route that references it, so one worker failure
cannot remain hidden in another maintained index. `WorkerStates` provides a
sorted detached health view for monitoring.

The router is immutable after construction and uses shared atomic health flags.
Selection allocates nothing and is safe concurrently with health changes. It
does not duplicate callbacks or automatically retry writes: callers must make
the update idempotent or otherwise safe before marking a failed worker
unhealthy. The feature is opt-in and does not alter existing placement or
worker-pool behavior.

## Tradeoff

Compared with a caller that already holds a direct replica slice, routing adds
an operator map lookup and atomic health checks. That cost buys explicit
cross-index worker failure propagation and deterministic failover without
introducing hidden duplicate side effects.
