# Dense Integer `IN` Sets

`hatSql` extends the prepared literal `IN` path with an automatic bounded
bitmap for dense integer lists. This is a narrow extension of the existing
typed binary-search optimization; it does not change SQL syntax or add a
configuration flag.

The bitmap is selected only when all of these conditions hold:

- the list has at least 16 literal `int` or `int64` values;
- values are within the exactly representable signed-integer range of
  `[-2^53, 2^53]` used by the existing numeric comparison rules;
- the inclusive value span is at most `1,048,576`; and
- the span is no more than 32 times the number of distinct values.

The representation retains a `uint64` bitmap and compact `int64` fallback
values. Integer and integral floating-point probes use constant-time bit
membership. Non-numeric values use the compact fallback so existing SQL
cross-type equality behavior is preserved. Lists containing NULL, strings,
unsupported numeric types, dynamic expressions, or sparse values retain the
existing linear or sorted-search evaluator.

The span limit is a hard memory guard. Preparation does extra sorting and
bitmap construction work, so the benefit is intended for compiled or reused
queries. The ordinary small-list, sparse-list, string-list, and dynamic-list
paths remain compatible with their previous behavior.

## Measurement

The paired 10,000-value lookup benchmark uses five `-benchtime=200ms` samples
on an AMD Ryzen 9 5950X Linux `amd64` host:

| Path | Median lookup | Program bytes | Lookup allocations | Relative result |
| --- | ---: | ---: | ---: | --- |
| Existing sorted search | 23.97 ns/op | 160,000 | 0 B/op, 0 allocs/op | 1.00x |
| Dense integer bitmap | 6.527 ns/op | 81,256 | 0 B/op, 0 allocs/op | 3.67x faster, 1.97x smaller |

Preparation is the tradeoff: the sorted baseline is 129,333 ns/op,
164,024 B/op, and 5 allocations; bitmap preparation is 196,606 ns/op,
83,352 B/op, and 5 allocations. That is 1.52x slower preparation while
cutting transient bytes by 1.97x. The optimization is therefore valuable when
the prepared expression is reused, not when a query is parsed only once.

Run the reproducible checks with:

```text
make test-chu17-c240
make test-chu17-package-c240
make race-chu17-c240
make vet-chu17-c240
make benchmark-chu17-c240
```
