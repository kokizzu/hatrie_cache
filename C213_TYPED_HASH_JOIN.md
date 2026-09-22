# C213: Typed Hash-Join Buckets

The ordinary SQL equality-hash-join path previously encoded every supported
join key into a canonical `string` and stored it in `map[string][]int`. The
existing C212 typed probe index now serves this path too. Numeric values keep
their IEEE payload bits, strings stay native `string` keys, and booleans use
two direct posting lists. The index is allocated lazily for each key family.

This is a narrow internal execution optimization. It does not change SQL
syntax, persistence, wire formats, resource limits, join ordering, or the
default choice between index, range, lookup, spill, nested-loop, and hash
joins. `NULL` and unsupported values remain non-matching exactly as before.
`EXPLAIN ANALYZE` labels the path `TYPED HASH JOIN`.

## Correctness

`TestC213SQLHashJoinUsesTypedIndex` covers duplicate string keys, booleans,
cross-type numeric equality, `NULL` non-matches, output order, and the plan
label. `TestC213SQLHashJoinPreservesLeftNullExtension` covers unmatched and
NULL left rows in a `LEFT JOIN`. Existing C212 canonical-key equivalence tests,
hash/index plan tests, range-join tests, and runtime join-filter tests remain
in the focused verification target.

Commands:

```text
make test-c213-typed-hash-join
make verify-c213-typed-hash-join
make race-c213-typed-hash-join
make vet-c213-typed-hash-join
```

## Measurement

The benchmark uses the same 2,048-row left input, 2,048-row right input, 1,024
numeric keys, duplicate matches, SQL query, resolver, five samples, and
`-benchmem` before and after the change on Linux/amd64 with an AMD Ryzen 9
5950X.

```text
make benchmark-c213-typed-hash-join
```

Raw samples:

| Path | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Before | 4,328,332 | 5,020,329 | 28,737 |
| Before | 4,157,993 | 5,020,329 | 28,737 |
| Before | 4,188,634 | 5,020,328 | 28,737 |
| Before | 4,002,143 | 5,020,328 | 28,737 |
| Before | 4,078,349 | 5,020,324 | 28,737 |
| After | 3,885,053 | 4,922,363 | 24,642 |
| After | 3,678,614 | 4,922,320 | 24,642 |
| After | 3,598,832 | 4,922,322 | 24,642 |
| After | 3,692,651 | 4,922,323 | 24,642 |
| After | 3,613,665 | 4,922,324 | 24,642 |

| Metric | Before median | After median | Result |
| --- | ---: | ---: | --- |
| CPU | 4,157,993 ns/op | 3,678,614 ns/op | 1.13x faster, 11.5% lower |
| Transient bytes | 5,020,328 B/op | 4,922,323 B/op | 1.02x lower, 2.0% lower |
| Allocations | 28,737 allocs/op | 24,642 allocs/op | 1.17x fewer, 14.2% lower |

The separate numeric/string maps are a deliberate small tradeoff: a workload
that mixes all three key families may retain multiple maps instead of one
canonical map. The benchmark shows lower transient memory and allocation cost
for the measured numeric workload, and the index's lazy family allocation
avoids paying for unused key types. Broader join shapes continue using their
existing specialized paths.
