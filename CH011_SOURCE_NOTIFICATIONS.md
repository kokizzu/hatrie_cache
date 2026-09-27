# CH-011 Source Notifications

This adds an opt-in, event-driven refresh queue for `MaterializedViews`. It is
useful when a source already emits a change notification and a periodic refresh
would add avoidable latency or duplicate work.

```go
queue, err := hatSql.NewMaterializedViewRefreshQueue(
    views,
    resolver,
    hatSql.QueryOptions{},
    hatSql.MaterializedViewRefreshQueueOptions{
        MaxPendingSources: 4096,
    },
)
if err != nil {
    return err
}

if err := queue.Notify(ctx, []string{"orders"}, hatSql.MaterializedViewRefreshMetadata{
    IdempotencyKeys: []string{"source-batch-42"},
}); err != nil {
    return err
}

// Run is normally owned by one maintenance goroutine. RunOnce is useful for
// an existing event loop or deterministic tests.
if err := queue.Run(ctx); err != nil {
    return err
}
```

Notifications are deduplicated by source while a batch is pending. A change
arriving during a refresh is retained for the next batch. Failed or canceled
refreshes are requeued, so the queue does not silently lose source changes.
The queue is bounded by distinct source keys; overflow is rejected without
changing the existing queue. The default is `4096` pending/in-flight sources.

This is deliberately opt-in. The existing synchronous `RefreshChanged` path
and all default query behavior are unchanged. Intermediate source changes may
be coalesced, so callers should use this queue only when the materialized view
needs the latest published state rather than one snapshot per source event.

## Measurement

Command:

```text
make benchmark-ch011-source-notifications
```

The script uses `-benchtime=50x -count=5` with `-benchmem`. Samples below are
the raw output from one run on AMD Ryzen 9 5950X, Linux/amd64.

| Benchmark | Samples (ns/op) | Median ns/op | B/op | Allocs/op |
| --- | --- | ---: | ---: | ---: |
| Direct synchronous refresh | 8400, 9164, 9089, 8793, 7220 | 8793 | 5254-5255 | 29 |
| Queue notify + one refresh | 9160, 6970, 9146, 10014, 7584 | 9146 | 5393-5398 | 31 |
| Eight notifications, one coalesced refresh | 11603, 8943, 10391, 10810, 14672 | 10391 per eight-event batch | 5376-5377 | 36 |

The single-event queue path is about 4% slower than direct refresh, with about
142 additional bytes and two additional allocations per operation. That is an
intentional opt-in coordination cost, not a claimed single-event speedup.

The coalesced case performs one refresh for eight notifications. Its median
batch time is about 6.8x lower than eight independent direct refreshes
(`8 * 8793 ns`), while publishing only the final state. This is the useful
win for bursty sources; workloads that require every intermediate snapshot
should keep the direct path.

## Verification

```text
make test-ch011-source-notifications
make race-ch011-source-notifications
make vet-ch011-source-notifications
```

Coverage includes source coalescing, idempotency-key retention, failed and
canceled batch retry, in-flight notification retention, bounded overflow,
invalid options, and the direct/queued benchmark comparison.
