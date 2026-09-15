# TR-053 Compact Secondary-Index Posting Lists

This Tarantool- and ClickHouse-inspired optimization stores the first row ID
of a secondary-index posting directly in the map value. A second ID creates a
small overflow object, and only later IDs need a slice. An absent map key still
represents an empty posting, so ID `0` remains valid.

The representation is shared by `FunctionalIndex`, non-unique `HashIndex`,
`ConditionalFunctionalIndex`, and `StringMultikeyIndex`. Posting order and
duplicate handling are unchanged: functional indexes retain insertion order,
while hash and multikey indexes retain sorted ID order. Deletes remove empty
postings and collapse a multi-ID posting back to the inline form.

No public API, wire format, or storage format changed. Unique hash indexes do
not use posting lists and are unchanged.

## Measurement

Linux/amd64, AMD Ryzen 9 5950X. Each row is the median of three benchmark
samples for building 10,000 rows. `B/op` and `allocs/op` are Go benchmark
allocation metrics. The baseline was captured before the implementation and
the after values use the final overflow layout.

| Index and distinct keys | Before | After | CPU improvement | Allocation bytes | Allocations |
| --- | ---: | ---: | ---: | ---: | ---: |
| Functional, 10,000 | 1.263 ms/op | 0.823 ms/op | 1.54x faster | 1,173,031 -> 873,891 (25.5% lower) | 10,070 -> 70 (143.9x fewer) |
| Hash, 10,000 | 1.335 ms/op | 0.840 ms/op | 1.59x faster | 1,173,047 -> 873,909 (25.5% lower) | 10,070 -> 70 (143.9x fewer) |
| Functional, 1,000 | 1.058 ms/op | 1.053 ms/op | 1.00x | 1,341,031 -> 1,025,894 (23.5% lower) | 5,070 -> 5,070 |
| Hash, 1,000 | 1.190 ms/op | 1.048 ms/op | 1.14x faster | 1,341,047 -> 1,025,909 (23.5% lower) | 5,070 -> 5,070 |
| Functional, 100 | 0.936 ms/op | 0.965 ms/op | 0.97x, 3.1% slower | 1,297,030 -> 1,081,096 (16.7% lower) | 870 -> 970 (11.5% more) |
| Hash, 100 | 1.047 ms/op | 1.047 ms/op | 1.00x | 1,297,047 -> 1,081,110 (16.7% lower) | 870 -> 970 (11.5% more) |
| Functional, 10 | 0.880 ms/op | 0.934 ms/op | 0.94x, 6.2% slower | 1,345,112 -> 1,126,293 (16.3% lower) | 190 -> 200 (5.3% more) |
| Hash, 10 | 1.098 ms/op | 0.993 ms/op | 1.11x faster | 1,345,128 -> 1,126,309 (16.3% lower) | 190 -> 200 (5.3% more) |

The compact form is a net win for sparse/high-cardinality indexes and lowers
allocation bytes for every tested distribution. Repeated-key functional
indexes can be a few percent slower and allocate one additional overflow
object per heavily populated key; that tradeoff is retained in this report so
future workload-specific measurements can revisit it.

Run the reproducible benchmark with:

```text
make benchmark-compact-postings-c204
```

Correctness and race coverage are available with:

```text
make verify-compact-postings-c204
```
