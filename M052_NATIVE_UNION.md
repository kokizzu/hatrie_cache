# M052 Native Scalar Set-Operation Fragments

M052 now composes the existing automatic native scalar dataflow executor across
top-level `UNION`, `UNION ALL`, `INTERSECT`, and `EXCEPT` branches when every
branch is a supported ordinary row-resolver scalar query. The parent query and
each eligible branch use the same native scan/filter/project runtime; the
existing set-operation assembly still owns duplicate counts, collation, and
spill limits.

This is deliberately conservative. The composed path stays disabled for CTEs,
joins, aggregates, windows, specialized resolvers, subquery-result caching,
frontier/snapshot controls, intermediate-row accounting, and explicit
`DisableNativeDataflow`. Those cases retain the existing executor.

## Correctness

The focused tests cover:

- `UNION ALL` with native execution in both branches;
- `UNION`, `INTERSECT ALL`, and `EXCEPT ALL` duplicate semantics;
- identical results with `DisableNativeDataflow: true`.

The native plan is also reported for each composed branch through the existing
observer/metrics path. No storage format, wire format, or public query option
changed.

## Measurement

Command: `make benchmark-m052-union-native` on Linux/amd64, AMD Ryzen 9 5950X,
one `-benchmem` sample per subbenchmark. The query scans 4,096 rows through
two scalar `UNION ALL` branches.

| Path | ns/op | B/op | allocs/op | Relative |
| --- | ---: | ---: | ---: | --- |
| Pre-change automatic path (general executor) | 5,900,937 | 6,114,250 | 49,192 | control before implementation |
| Final general-executor control | 5,732,191 | 6,114,274 | 49,192 | 1.00x |
| Final automatic native branches | 3,838,696 | 2,134,967 | 32,790 | 1.50x faster, 65.1% less heap, 33.3% fewer allocations |

Against the pre-change automatic path, the final native path is 1.54x faster,
uses 2.86x less heap, and performs 1.50x fewer allocations. These are
single-sample directional measurements; rerun the target on the deployment
machine before using them as capacity estimates.

## Verification

```text
make test-m052-union-native
make test-m052c-native-dataflow
make test-race-m052c-native-dataflow
make vet-m052c-native-dataflow
make benchmark-m052-union-native
```

The isolated repository-wide run passed `hat/hatSql`, including this feature.
It still reports unrelated existing `hatPeer` compact-protocol and mutual-TLS
failures, including a 10-minute timeout; those are outside this change.
