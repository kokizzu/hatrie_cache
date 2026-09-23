# M220: Safe Materialized Index Removal

## Purpose

M220 adds selective removal of materialized-view point postings without
dropping the materialized rows. This is useful when a workload changes, an
index is no longer selective, or memory needs to be returned while the view
itself remains useful for arrangement scans and exact snapshot reads.

## API

```go
// Remove one point index while retaining the materialized view rows.
if err := views.DropPointLookupFields("people_view", "region"); err != nil {
	return err
}

// Remove every point index from the view.
if err := views.DropPointLookupFields("people_view"); err != nil {
	return err
}
```

The operation is idempotent for fields that are already absent. An empty view
name or field name is rejected, and a missing view returns an error. Unknown
field names are ignored when other configured fields remain. The snapshot rows,
source versions, and aggregate storage accounting are unchanged.

After removal, `PointLookup` reports the field as unavailable. A compatible
filtered SQL query remains correct and uses the existing
`MATERIALIZED ARRANGEMENT SCAN` path. To add an index again without rebuilding
the view, use the M219 `EnqueuePointLookupBuild` API with the desired fields.

## Reader Drain And Publication

Each published view owns a small reader gate shared by copied immutable
generations. A point lookup acquires the gate before it captures the posting
map and releases it after rows are cloned. Removal takes the registry write
lock, waits for readers of that view's generation, then publishes a new view
value with the selected postings omitted. It never mutates a map that an
in-flight reader can be using.

The registry lock is released before row cloning and arrangement scanning, so a
long read of one view no longer holds the global materialized-view read lock
against unrelated view publication or removal. Refreshes continue to replace
complete snapshot values atomically; an already admitted reader finishes on its
old immutable generation.

The removed posting map becomes eligible for garbage collection after the last
reader holding that generation drains. No partially removed index is visible.
The tradeoff is one reader-gate pointer and mutex per view, plus a removal that
waits for current readers of the affected view.

## Measurement

The benchmark uses a 4,096-row view with a `region` point index. Both runs use
the same Linux/amd64 host and the default benchmark settings through
`make m220-benchmark`. The baseline is one pre-change capture; the after
values below are medians from five post-change captures because this small
lookup is sensitive to host scheduling.

| Workload | Before ns/op | After ns/op | Before B/op | After B/op | Before allocs/op | After allocs/op | Result |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Single point lookup | 17,972 | 17,421 | 22,032 | 22,032 | 130 | 130 | 1.03x faster, same allocation profile |
| Parallel point lookup | 6,487 | 6,317 | 22,032 | 22,032 | 130 | 130 | 1.03x faster, same allocation profile |

The observed CPU improvement is modest and should be treated as a directional
result rather than a universal guarantee. The important memory behavior is
retained-state reduction after removal; `B/op` deliberately does not measure
that lifetime effect, and no claim of a retained-heap multiplier is made here.

Raw benchmark output:

```text
Before:
BenchmarkM220MaterializedViewIndexLifecycle/point_lookup-32             62053  17972 ns/op  22032 B/op  130 allocs/op
BenchmarkM220MaterializedViewIndexLifecycle/point_lookup_parallel-32   161161   6487 ns/op  22032 B/op  130 allocs/op

After samples:
point_lookup ns/op: 16883 18585 16243 17421 18506
point_lookup_parallel ns/op: 6317 6858 6243 5883 7947
B/op: 22032 on every sample; allocs/op: 130 on every sample
```

## Verification

Focused tests cover preserving other indexes, arrangement-scan fallback,
removing all indexes, concurrent readers during removal, and a deterministic
reader-gate drain. Run:

```text
make m220-test
make m220-race
make m220-vet
make m220-benchmark
```
