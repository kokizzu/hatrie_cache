# M222: Replicated Index Rebuild Workers

M222 adds an opt-in replica coordinator for maintained-index rebuilds. It fans
one revision-fenced `SQLIndexRebuildRequest` to multiple bounded
`SQLIndexRebuildQueue` instances and reports both the quorum result and every
replica result.

## API

Create independent queues and choose the minimum number of successful workers
needed for logical success:

```go
first, _ := hatSql.NewSQLIndexRebuildQueue(hatSql.SQLIndexRebuildQueueOptions{Workers: 1})
second, _ := hatSql.NewSQLIndexRebuildQueue(hatSql.SQLIndexRebuildQueueOptions{Workers: 1})
replicas, _ := hatSql.NewSQLIndexRebuildReplicaSet(hatSql.SQLIndexRebuildReplicaSetOptions{
    Queues: []*hatSql.SQLIndexRebuildQueue{first, second},
    Quorum: 1,
})

_ = replicas.Start(ctx)
status, err := replicas.Enqueue(hatSql.SQLIndexRebuildRequest{
    ID:   "people-index-v1",
    Name: "people_projection",
    Run:  buildPeopleIndex,
})
_ = replicas.Flush(ctx)
status, _ = replicas.Status("people-index-v1")
```

`Quorum: 0` defaults to one. With two queues and quorum one, either worker can
make the logical operation succeed while the other accepted worker continues
to converge its state. A quorum equal to the queue count requires every active
worker to succeed.

`Start` attempts every queue. If fewer queues start than the quorum it returns
`ErrSQLIndexRebuildReplicaQuorumUnavailable`; queues that did start remain
usable. After start, an inactive queue is represented as a failed replica and
does not make `Flush` block when the configured quorum is still possible. If an
enqueue cannot possibly satisfy quorum, it returns the same error and cancels
any already accepted replica tasks.

## Materialized Views

`MaterializedViews.EnqueueReplicatedPointLookupBuild` uses the same coordinator
for point-posting hydration. Each worker captures the same snapshot revision
and desired fields. Publication remains revision fenced and idempotent, so a
successful worker cannot install an index for a newer or removed view. The
existing `HydrationStatus` call reports `hydrating` until the replica operation
reaches quorum and then reports `ready`.

## Correctness Requirements

The `Run` and optional `Verify` callbacks are invoked once per accepted
replica. They must therefore be idempotent, or protect their side effects with
the request's revision/generation identity. A replica set is an in-process
coordinator; placing queue execution in separate processes requires the caller
to provide the transport and process supervision around those queues.

Cancellation is cooperative. A running callback must observe its context and
return promptly. A callback that ignores cancellation can keep its own worker
busy even though the coordinator marks the logical operation unavailable.

## Cost And Measurement

Replication is an availability feature, not a speed optimization. Accepted
replicas repeat the rebuild work and require their own queue/task state. With
two queues, the measured control-plane operation was:

| Path | Median ns/op | B/op | allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Direct single queue | 16.38 | 0 | 0 | 1.00x |
| Two-replica coordinator | 645.9 | 1,088 | 3 | 0.025x (about 39.4x slower) |

The benchmark measures enqueue/status coordination, not the rebuild callback
itself. The extra CPU and memory are the explicit cost of fanout and per-replica
status tracking. Keep the default single queue when high availability is not
needed.

Raw samples and the exact command are recorded in
[BENCHMARK.md](BENCHMARK.md#m222-replicated-index-workers).

## Verification

```text
make m222-test
make m222-race
make m222-vet
make m222-benchmark
```

The focused tests cover one-worker failure, quorum handling, inactive-worker
flush behavior, partial-acceptance cancellation, and materialized-view
publication on all successful replicas.
