# M052ad Automatic Native Conditional Aggregates

This feature adopts the ClickHouse-style conditional aggregate family in the
automatic SQL native-dataflow selector. It lowers the supported forms into the
existing constant-state grouped aggregate runtime without changing SQL syntax
or the public fallback switch.

## Supported Forms

For eligible single-source `CACHE` and `KEYS` queries over ordinary row
resolvers, the automatic path supports:

- `COUNT_IF(condition)` and `COUNTIF(condition)`
- `SUM_IF(value, condition)` and `SUMIF(value, condition)`
- `AVG_IF(value, condition)` and `AVGIF(value, condition)`
- `MIN_IF(value, condition)` and `MINIF(value, condition)`
- `MAX_IF(value, condition)` and `MAXIF(value, condition)`

The condition and value expressions must be supported scalar expressions. The
native path remains fail-closed for windows, joins, richer grouped shapes,
unsupported functions, and specialized resolver contracts.

`DisableNativeDataflow: true` remains the explicit fallback switch. An
ordinary aggregate using `FILTER (WHERE ...)` is not treated as a conditional
function and remains on the established evaluator unless its shape is
explicitly supported in a later feature.

## Correctness Boundary

The parser normalizes conditional aggregates into their base aggregate plus an
aggregate filter. The native lowering recognizes the original conditional
function token and attaches that filter to the existing aggregate state.

The grouped hash fast path now rejects filtered aggregate expressions instead
of silently ignoring their filters. This changes the old behavior for those
queries from an incorrect result to the correct materialized fallback. The old
path is therefore not a valid performance baseline for conditional aggregates.

## Benchmark

Command:

```text
make benchmark-m052ad-conditional-aggregate
```

The benchmark uses 20,000 rows, 257 groups, one conditional count, and one
conditional sum. Results are Linux/amd64 on an AMD Ryzen 9 5950X, with
`-benchmem -count=3`; the table reports medians from the three samples.

| Path | ns/op | B/op | allocs/op | Relative to correct fallback |
| --- | ---: | ---: | ---: | --- |
| Correct fallback | 17,817,491 | 31,145,692 | 137,758 | 1.00x |
| Automatic native conditional aggregate | 5,286,292 | 7,622,228 | 60,881 | **3.37x faster; 4.09x lower bytes; 2.26x fewer allocations** |

Raw samples:

```text
fallback:
17817491 ns/op 31145881 B/op 137758 allocs/op
17801243 ns/op 31145692 B/op 137757 allocs/op
17711667 ns/op 31145461 B/op 137758 allocs/op

automatic:
5194889 ns/op 7622228 B/op 60881 allocs/op
5286292 ns/op 7622226 B/op 60881 allocs/op
5303246 ns/op 7622228 B/op 60881 allocs/op
```

## Verification

The feature-specific tests compare automatic and fallback rows for all six
supported aggregate kinds, including the compact `COUNTIF` spelling. They also
verify that an explicit aggregate filter remains off the native path. The
following gates passed:

```text
make test-m052ad-conditional-aggregate
make test-m052ad-package
make race-m052ad-package
make vet-m052ad-package
```
