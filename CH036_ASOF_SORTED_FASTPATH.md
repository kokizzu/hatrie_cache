# CH-036 ASOF JOIN Sorted-Bucket Fast Path

The SQL ASOF JOIN implementation groups right-side rows by equality key and
then binary-searches each temporal bucket. Before this change it called a
stable sort for every bucket, including sources already ordered by time.

The executor now scans each bucket for monotone nondecreasing timestamps and
only sorts when an out-of-order timestamp is found. Equal timestamps retain
the old stable input order. Unordered sources therefore keep the previous
semantics and sort fallback.

## Verification

Focused correctness target:

```text
make test-ch036-asof-sorted-fastpath
```

It covers ordered and unordered bucket preparation plus the existing SQL ASOF
inner and left-join behavior. Race and vet checks run with:

```text
make verify-ch036-asof-sorted-fastpath
```

## Benchmark

Host: Linux/amd64, AMD Ryzen 9 5950X. Fixture: 32 equality buckets with 256
right-side candidates each, five one-second samples per benchmark, `-benchmem`.
The baseline always sorts; the fast path scans and skips sorting when ordered.

| Workload | Baseline median | Fast-path median | CPU change | Memory | Allocations |
| --- | ---: | ---: | ---: | ---: | ---: |
| Already ordered | 297,791 ns/op | 235,813 ns/op | 1.26x faster | 1,059,256 -> 1,052,523 B/op | 143 -> 44 allocs/op |
| Reversed input | 2,508,543 ns/op | 2,496,285 ns/op | 1.00x, within noise | 1,071,115 -> 1,070,802 B/op | 238 -> 237 allocs/op |

Raw command:

```text
make benchmark-ch036-asof-sorted-fastpath
```

The optimization is kept because the common ordered case is materially faster
and smaller, while the unsorted fallback has no measured cost increase and no
semantic change.
