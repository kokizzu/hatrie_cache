# TT-023 String Equality Index Fast Path

## What changed

Ordinary SQL JSON field indexes already use a hash map for exact equality. For
an index whose non-null values are all strings, the map now stores the string
itself as the key instead of allocating the legacy `s:`-prefixed key on every
probe. The shared lookup helper is used by the copying resolver, the borrowed
resolver, and cardinality estimation.

When a field contains a non-string value, the rebuild converts string keys to
the existing `s:` namespace and keeps the previous mixed-type behavior. Null,
missing, unsupported values, refresh generation checks, and result ordering
are unchanged. Prefix, range, grouping, and ordering paths continue to use
their existing ordered index data.

The optimization is internal and requires no configuration or migration. It
does not add a second retained map or sidecar, so homogeneous string indexes
do not pay a permanent memory multiplier.

## Verification

The regression coverage includes raw-key refresh, mixed string/integer keys,
type separation, non-string misses, and zero-allocation repeated lookup. The
following repository targets passed:

```text
make test-tt023
make test-tt023-package
make race-tt023
make vet-tt023
make format-tt023
```

## Measurement

Machine: AMD Ryzen 9 5950X, Linux amd64. Workload: one repeated lookup in a
one-row ordinary field-index map, measured with `-benchmem` and five runs.

| Variant | Median time | Allocated bytes | Allocations |
| --- | ---: | ---: | ---: |
| Legacy `s:` key construction, before change | 45.20 ns/op | 8 B/op | 1 alloc/op |
| Raw string key, after change | 10.49 ns/op | 0 B/op | 0 allocs/op |

The same post-change benchmark run measured the legacy path at a 40.84 ns/op
median and the optimized path at 10.49 ns/op, a conservative 3.90x lookup
speedup under identical process conditions. Compared with the pre-change
five-run median, the improvement is 4.31x. This benchmark isolates exact map
lookup and key creation; it does not claim a 4x speedup for full SQL execution
or index rebuilds.

Raw runs, in the order emitted by `go test`:

```text
before legacy: 45.20 45.84 45.28 44.42 43.61 ns/op
after legacy:  40.84 38.77 40.58 41.12 40.98 ns/op
after fast:    10.57 10.07  9.881 10.49 10.54 ns/op
```
