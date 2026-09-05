# Query Subscription Status

`QuerySubscriptions.Status(observedFrontier)` returns a deterministic,
point-in-time list of active subscription progress. Each
`QuerySubscriptionStatus` contains the subscription ID, result revision,
frontier, clamped lag, completion flag, and the number of currently queued
updates.

```go
statuses := subscriptions.Status(sourceFrontier)
for _, status := range statuses {
    metrics.Set(status.ID, status.Lag, status.QueuedUpdates)
}
```

The method is read-only and caller-driven. It does not start workers, retain a
history, or change notification behavior. Closed subscriptions disappear
from later snapshots, and results are sorted by ID for stable metrics and
tests. `Lag` is `observedFrontier - Frontier` when the observed frontier is
newer, otherwise zero.

This is an on-demand per-subscription progress surface. It does not yet claim
per-source lag aggregation; callers that maintain multiple source frontiers
should poll with the frontier relevant to the subscription group.

## Measurement

With 64 active subscriptions, `make benchmark-sql-subscription-status` reports
about `6.3-6.4 us/op`, `3,705 B/op`, and `5 allocs/op` on the repository
benchmark host. The allocation is limited to the returned status slice and
does not affect the notification hot path.
