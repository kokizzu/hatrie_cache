# M250 Temporal Join Frontier Alignment

Materialize-style temporal joins must not use one input at a timestamp that the
other input has not reached. `SQLTemporalJoinFrontierAlignment` is an opt-in
coordination primitive for callers that maintain separate left and right
`SQLSourceFrontierBarrier` instances.

```go
alignment, err := hatSql.NewSQLTemporalJoinFrontierAlignment(leftBarrier, rightBarrier)
if err != nil {
	return err
}

frontiers, err := alignment.WaitForFrontier(ctx, timestamp)
if err != nil {
	return err
}

// Use the pair only after both inputs reached timestamp.
_, err = temporalJoin.Compact(frontiers.LeftFrontier, frontiers.RightFrontier)
```

`WaitForFrontier` returns `LeftFrontier`, `RightFrontier`, and
`CommonFrontier`, where the common value is the minimum of both observed
frontiers. It distinguishes an observed frontier of zero from an unready
input, honors context cancellation, and wakes when either barrier publishes a
new observation. The constructor rejects nil barriers. `CommonFrontier` and
`ReadyAt` are available for nonblocking checks.

The already-ready path uses the barriers' atomic cached frontier state and
starts no goroutines. When either input is behind, the two waits run together
under a child context; an error or cancellation releases the other waiter.
The slow path therefore has coordination overhead and should be used for
actual cross-input temporal consistency, not for every independent point
read.

This feature is disabled unless a caller constructs the alignment and waits on
it. `SQLQueryOptions.RequireSourceFrontier` and ordinary source barriers keep
their existing behavior. A caller that already has one barrier containing all
input partitions may continue using that barrier directly.
