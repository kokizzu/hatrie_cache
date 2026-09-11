# Materialized View Dependency Invalidation

`MaterializedViews` now maintains a reverse dependency index while views are
created. The index maps each normalized source key to the names of views that
depend on it. `RefreshChanged` uses the index to collect only affected views,
deduplicates overlaps when several changed sources point to the same view, and
keeps the existing deterministic name sort and atomic refresh publication.

## Why

The previous refresh planner scanned every registered view for every change.
That made an update to one source pay for unrelated views and allocated a
candidate slice sized for the entire registry. The reverse index changes
candidate discovery to depend on the changed source fan-out rather than total
view count.

The index is direct-source based. View definitions already declare source-key
dependencies, and the executor does not treat one materialized view as another
view's source, so no transitive graph traversal is added. Failed refreshes
still leave every prior snapshot unchanged.

## Memory Cost

The registry retains one source-to-view map entry for each distinct dependency
and one view-name slice element for each dependency edge. This is bounded by
the definitions already registered and avoids a per-refresh full-registry
candidate allocation. The index is populated under the same registry lock as
the view publication.

## Measured Result

Five `-benchmem` samples were run on `linux/amd64`, AMD Ryzen 9 5950X. Each
case refreshes one changed source against the indicated number of views; the
one-match case executes one actual refresh and the no-match case executes none.

| Views | Case | Scan median | Indexed median | Relative | Heap / allocs |
| ---: | --- | ---: | ---: | --- | --- |
| 8 | one match | 4,592 ns/op | 3,936 ns/op | 1.17x faster | 5,976 -> 3,960 B / 27 -> 27 |
| 8 | no match | 664.4 ns/op | 131.6 ns/op | 5.05x faster | 2,328 -> 24 B / 2 -> 1 |
| 1,024 | one match | 101,225 ns/op | 4,846 ns/op | 20.89x faster | 282,201 -> 3,960 B / 27 -> 27 |
| 1,024 | no match | 90,684 ns/op | 133.9 ns/op | 677x faster | 278,552 -> 24 B / 2 -> 1 |
| 4,096 | one match | 381,645 ns/op | 4,645 ns/op | 82.11x faster | 1,117,788 -> 3,960 B / 27 -> 27 |
| 4,096 | no match | 375,706 ns/op | 132.1 ns/op | 2,844x faster | 1,114,136 -> 24 B / 2 -> 1 |

The indexed path is automatic because it preserves exact refresh semantics and
the small-registry cost is lower in the measured fixture. Run the focused
checks with:

```text
make test-mz042-dependency-graph
make benchmark-mz042-dependency-graph
```
