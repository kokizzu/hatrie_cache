# Partition Resize Planning

`hatPartition` provides deterministic planning for one-step local partition
splits and merges. This is an operator and migration-tooling primitive. It
does not move records, change a running cache, start a worker, or enable
partitioning.

## Supported Layouts

The existing local router accepts zero (disabled) or a power of two from 2
through 256. Resize plans require an enabled layout and one adjacent change:

- `PlanSplit(4)` plans `4 -> 8`.
- `PlanMerge(8)` plans `8 -> 4`.
- `PlanResize(from, to)` accepts either adjacent direction and rejects all
  other layouts.

Splitting source partition `i` produces target partitions `i` and `i+N`, where
`N` is the source count. Merging target partition `i` consumes source
partitions `i` and `i+N`. The lower-bit hash routing remains the same, so a
split can leave some keys in place while moving only the keys selected by the
new bit.

## Go API

```go
plan, err := hatPartition.PlanSplit(16)
if err != nil {
    return err
}

source, target, ok := plan.RouteKey(key)
if ok && source != target {
    // Copy key from source to target, then verify the destination.
}
```

`RouteKey` computes both partitions with one `xxhash` invocation and does not
allocate. `SourcePartition`, `TargetPartition`, and `MovesKey` are convenient
single-purpose variants. `Moves` returns the complete deterministic mapping
for operator inspection; it allocates once for that mapping and should not be
used per record.

`ResizePlan` values are immutable after construction. The zero value is safe:
its accessors report zero, partition lookups return `-1`, and `Moves` returns
`nil`.

## Safe Operator Workflow

1. Keep `local_partitions` at its existing value and create a backup before a
   planned migration.
2. Create a `PlanSplit` or `PlanMerge` for the adjacent target layout.
3. Enumerate records from the source layout and use `RouteKey` to create a
   deterministic copy plan. Keys whose source and target are equal can remain
   in place.
4. Copy each moved record with the normal write path and preserve its value,
   TTL, and journal requirements.
5. Verify destination lookups and backup checksums before changing deployment
   configuration.
6. Restore or roll back from the backup if verification fails.

The library intentionally does not provide automatic online movement yet.
Ownership fencing, concurrent writes, replication cutover, and resumable
journal handling remain separate concerns and are not silently guessed by this
API. Automatic sharding and vshard-style routing remain disabled and deferred.

## Cost Measurement

Measured on the repository benchmark host (`AMD Ryzen 9 5950X`, Linux amd64),
with `go test ./hat/hatPartition -run '^$' -bench ... -benchmem -count=5`:

| Operation | Median ns/op | B/op | Allocs/op | Relative |
| --- | ---: | ---: | ---: | ---: |
| Existing `Index` before feature | 11.65 | 0 | 0 | 1.00x |
| Existing `Index` after feature | 12.18 | 0 | 0 | 1.05x |
| `ResizePlan.TargetPartition` | 13.80 | 0 | 0 | 1.13x |
| `ResizePlan.RouteKey` | 14.80 | 0 | 0 | 1.22x vs one `Index` |
| Two separate planned lookups | 30.64 | 0 | 0 | `RouteKey` is 2.07x faster |

The small single-lookup overhead is the cost of a runtime plan mask and
zero-value validity check. It is paid only by callers performing a resize
plan. Existing cache routing and the default partition count are unchanged.

Run the repeatable checks with:

```text
make test-partition-resize
make benchmark-partition-resize
```
