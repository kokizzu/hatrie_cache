# Explicit Write Quorum

`hatReplication.ExecuteWriteQuorum` is an opt-in executor for writes that must
reach an explicit number of named replicas before the caller treats the write
as durable enough. It runs all listed target callbacks concurrently, returns a
deterministic result in input order, and leaves the existing asynchronous
replication path unchanged.

```go
result, err := hatReplication.ExecuteWriteQuorum(
    ctx,
    []string{"local", "east", "west"},
    2,
    func(ctx context.Context, node string) error {
        return writeToReplica(ctx, node, payload)
    },
)
if err != nil {
    // Inspect result.Attempts and reconcile failed replicas.
    return err
}
```

## Contract

- The node list must be non-empty, contain non-empty unique names, and
  `required` must be between one and the number of nodes.
- The callback must be non-nil and safe for concurrent calls.
- All supplied targets are attempted, even after the required acknowledgement
  count is reached, so failed replicas are visible for repair.
- A nil callback error counts as an acknowledgement. Callback errors are
  returned in the corresponding `WriteQuorumAttempt.Error` string and do not
  stop other target attempts.
- A satisfied quorum returns a nil error. An unsatisfied quorum returns
  `ErrWriteQuorumUnsatisfied` with the partial decision.
- A context canceled before execution returns
  `ErrWriteQuorumContextCanceled` and invokes no callback. Cancellation during
  callbacks is passed through the context; callbacks that already completed
  successfully still count as acknowledgements.

This executor does not roll back successful replica writes when another target
fails. The caller owns repair/reconciliation and should use an idempotent
transaction identifier when retries are possible. For sink retries,
`hatSql.SQLSinkCommitCoordinator` can provide the idempotency gate, but neither
component can make arbitrary external side effects atomic by itself.

## Public Command Path

`hatCache.MonitoringOptions.WriteQuorum` and
`hatCache.CacheGRPCOptions.WriteQuorum` enable the same policy for single public
write commands served through the monitoring HTTP endpoint or unary gRPC
command endpoint. The threshold includes the local command result. The default
is `0`, which preserves the existing asynchronous or best-effort replication
behavior.

Command-path quorum requires a direct replicator with `AsyncQueueSize == 0`.
With a positive threshold, invalid asynchronous configuration and canceled
contexts are rejected before the local command runs. For a valid direct
replicator, the local command is applied before remote acknowledgements are
collected. If the threshold is not met, the response reports
`ErrWriteQuorumUnsatisfied`, but the local write remains applied; callers must
reconcile or retry using an idempotent command. Non-eligible `BATCH` and
internal replication commands retain their existing transaction and
replication paths.

An atomic public `BATCH` also honors `WriteQuorum` when every item is an eligible
journaled write command with a key. The batch is validated before local
mutation, committed or rolled back locally using the existing atomic batch
logic, and sent as one grouped `INTERNALBATCHV2` per replication target. A
remote quorum failure does not roll back the local atomic commit; rollback-free
cluster-wide commit is not provided by this path.

## Measured Cost

Measured with `go test ./hat/hatReplication -run '^$' -bench
'BenchmarkExecuteWriteQuorum' -benchmem -count=5` on Linux/amd64 with an AMD
Ryzen 9 5950X and three no-op target callbacks:

| Path | Time | Heap | Allocs |
| --- | ---: | ---: | ---: |
| Three-target quorum, required two | 1.235-1.288 us/op | 544 B/op | 10/op |
| Public command, quorum disabled | 362.2-371.0 ns/op | 0 B/op | 0/op |
| Public command, loopback quorum required two | 101.8-106.1 us/op | 14.4-14.6 KB/op | 161/op |

The public-command measurements used one loopback HTTP target and five
`-count=5` samples. The median was `363.4 ns/op` with quorum disabled versus
`103.8 us/op` with quorum enabled, or about `286x` slower; this is the expected
durability latency and is why the setting remains opt-in.

The cost includes one goroutine and result bookkeeping per target. This is a
durability/resilience feature, not a throughput optimization; it is disabled
unless the caller explicitly invokes it.
# Opt-in early acknowledgement

`hatReplication.ExecuteWriteQuorumUntilSatisfied` is an opt-in variant for
latency-sensitive callers. It starts target callbacks concurrently and returns
when the required acknowledgements are available or cannot be reached. Pending
callbacks receive a canceled context, so callbacks must honor cancellation and
remain safe for concurrent calls. The existing `ExecuteWriteQuorum` function
still waits for every target and remains unchanged.

The returned `WriteQuorumEarlyResult` contains completed attempts in input
order and a `Pending` count for callbacks that were outstanding at return. A
successful early result does not imply that pending replicas applied the write;
callers that need repair details should continue using the wait-for-all API.

## Benchmark

Linux, AMD Ryzen 9 5950X, Go benchmark with three targets and `-benchmem`;
reported values are representative medians of three runs:

| Workload | ns/op | B/op | allocs/op | Result |
| --- | ---: | ---: | ---: | --- |
| Existing wait-for-all, all fast | 1,316 | 544 | 10 | Baseline |
| Early acknowledgement, all fast | 1,831 | 944 | 13 | 1.39x CPU, 1.74x bytes, 1.30x allocations |
| Existing wait-for-all, one 100 us slow target | 1,062,951 | 792 | 13 | Baseline |
| Early acknowledgement, one 100 us slow target | 2,913 | 1,303 | 16 | 364.9x lower latency, 1.65x bytes, 1.23x allocations |

The API is therefore deliberately opt-in: it is a large tail-latency win when
slow replicas are common, but it costs coordination overhead on an all-fast
path and returns before every replica has completed.
