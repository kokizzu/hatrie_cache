# CH-048 Literal Numeric NOT BETWEEN

Literal numeric `NOT BETWEEN` predicates now use a packed columnar kernel when
the predicate is a direct field with two numeric literals and the source has a
validated `int64` or `float64` packed column. The kernel preserves SQL null
behavior through the typed validity bitmap and treats NaN as the negation of
the existing comparison pair. Dynamic bounds, non-numeric literals, legacy
unpacked columns, and unsupported layouts retain the general evaluator.

The columnar eligibility proof accepts both `BETWEEN` and `NOT BETWEEN`, while
the specialized recognizer remains deliberately narrower. This keeps broader
expression semantics on the established evaluator. The first implementation
does not add segment pruning for the complement range; its win is eliminating
per-row interface evaluation and allocation.

## Verification

The focused test target covers recognizer validation, packed and legacy
semantics, NULL, NaN, reversed bounds, dynamic bounds, and end-to-end
columnar materialization:

```text
make test-ch048-not-between
```

## Benchmark

Workload: one matcher scans 99,840 packed numeric rows for
`value NOT BETWEEN 1024 AND 3071`. The matcher is constructed outside the
timed loop. Linux/amd64, AMD Ryzen 9 5950X, `-benchtime=300ms -count=3`.

| Version | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Existing general evaluator | 22,271,893; 22,527,211; 23,456,360 | 13,527,240; 13,527,221; 13,527,603 | 293,124; 293,124; 293,125 |
| Packed NOT BETWEEN kernel | 836,488; 780,671; 959,121 | 3; 3; 4 | 0; 0; 0 |

Median comparison: `22.53 ms` to `0.84 ms`, about `26.9x` lower CPU time;
`13.53 MB` to `3 B`, and `293,124` to `0` allocations. No wire or storage
format changes are involved.
