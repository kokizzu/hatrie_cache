# M220: Safe Point Lookup Retirement

## Decision

M220 adds explicit reader leases and asynchronous retirement for maintained
materialized-view point lookup indexes. `StartPointLookupIndexRetirement`
removes an index from new readers immediately, while already acquired readers
retain an immutable index snapshot until `Close` is called. Retirement then
reports `ActiveReaders` and completes at `retired`.

The existing one-shot `LookupPoint` path remains lock-protected and unchanged
for callers that do not need a multi-operation lease. The existing
`DropPointLookupIndex` API now starts retirement and waits for all dependent
readers, preserving its synchronous behavior while making removal safe.

## API

```go
reader, err := views.AcquirePointLookupReader("people_by_id")
if err != nil {
    return err
}
defer reader.Close()

retirement, err := views.StartPointLookupIndexRetirement("people_by_id")
if err != nil {
    return err
}

status := retirement.Status()
// Existing reader remains usable while new LookupPoint calls fail as missing.
result, found, err := reader.LookupPoint("42")
status, err = retirement.Wait(ctx)
```

Retirement states are `retiring` and `retired`. A reader must be closed even
when it has no more keys to read. Closing is idempotent. Acquiring a new reader
or recreating the same index while retirement is active returns
`ErrMaterializedViewPointLookupIndexRetirementInProgress`.

## Correctness

- New one-shot lookups are rejected as soon as retirement is published.
- Existing readers retain their immutable index snapshot and can finish safely.
- The retirement handle does not complete until every acquired reader closes.
- `DropPointLookupIndex` blocks until the same drain condition is satisfied.
- Dropping a materialized view marks its point indexes retiring, so an old
  reader cannot race with reuse of the same index name.
- Refreshes replace the registry's current index atomically; an already
  acquired reader continues to observe the snapshot it acquired.

## Measurement

Workload: 10,000 rows, integer point key, lookup key `9999`, ten iterations per
sample and five samples, AMD Ryzen 9 5950X. Retirement setup and index
construction are outside the timed retirement loop.

| Operation | Median ns/op | Median B/op | Median allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Existing one-shot lookup | 740.1 | 376 | 4 | baseline |
| Leased reader lookup | 596.0 | 376 | 4 | same bytes and allocations |
| Retirement with no readers | 5,512 | 456 | 4 | O(1) removal, one-off lifecycle cost |

Raw samples:

```text
one_shot_lookup ns/op:       542 413 740.1 1286 1001
leased_lookup ns/op:         639 603 381.1 500 596
retire_without_readers ns/op: 5315 5793 5512 7685 5356
```

The feature is a safety and lifecycle improvement, not a query-throughput
optimization. One-shot lookup memory and allocation measurements remain
unchanged in this benchmark. The cost is a small reader/retirement state per
maintained index and the requirement for long-lived consumers to call
`Close`.

## Verification

```text
make test-m220-point-lookup-retirement
make test-m220-related-materialized
make race-m220-point-lookup-retirement
make vet-m220-point-lookup-retirement
make benchmark-m220-point-lookup-retirement
```

The tests cover reader use during retirement, visible active-reader progress,
timeout without a drain, idempotent close, synchronous drop waiting, and
post-retirement lookup rejection.
