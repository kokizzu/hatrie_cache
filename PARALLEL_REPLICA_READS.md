# Parallel And Hedged Replica Reads

`hat/hatReplication` provides an opt-in first-success read coordinator for
the same query or lookup against multiple named replicas.

```go
result, err := hatReplication.ExecuteParallelReplicaRead(
	ctx,
	[]string{"replica-a", "replica-b", "replica-c"},
	25*time.Millisecond,
	func(ctx context.Context, node string) (any, error) {
		return client.Query(ctx, node, query)
	},
)
if err != nil {
	return err
}
return use(result.Node, result.Value)
```

The first node starts immediately. A zero hedge delay starts all nodes
immediately and returns the first successful result. A positive delay starts
one additional node per interval while no result has succeeded. A successful
attempt cancels remaining callbacks. When every replica fails, the function
returns `ErrParallelReplicaReadFailed` and the attempt report in input order.

If a replica fails and no other attempt is active, the next replica starts
immediately instead of waiting for the hedge timer.

`ParallelReplicaReadAttempt` distinguishes started and completed work, so a
caller can see which slow losers were canceled after the winner returned. Read
callbacks must honor their context; the coordinator cannot preempt a callback
that ignores cancellation. The callback remains responsible for authentication,
timeouts, query authorization, response validation, and replica freshness.

This API is separate from `ExecuteReadQuorum`: parallel reads optimize latency
by accepting the first success, whereas read quorum intentionally waits for
enough matching responses to make a consistency decision.

## Cost And Selection

Immediate fan-out consumes work on every started replica and should be used
only when that extra load is acceptable. Hedging reduces duplicate work when
the first replica is healthy, but adds timer and scheduling latency before a
backup starts. Choose the delay from observed replica tail latency and keep
the replica list bounded.

No existing replication routing or default query path changes. The function is
an importable coordinator for callers that already own the remote read path.

## Verification

```text
make test-parallel-replica-read-full-clean
make test-parallel-replica-read-race-clean
make benchmark-parallel-replica-read-clean
```
