# C225: Incremental Ordered Window Frames

## Status

C225 is implemented for the narrow running-aggregate shape where the frame
only grows: `SUM`, `AVG`, `MIN`, or `MAX` with one argument and either the
default frame or `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.

The executor prepares each row's partition key, ordering values, and argument
once, sorts each partition once, and updates aggregate state as rows advance.
This removes repeated frame scans and repeated expression evaluation.

## Compatibility Boundary

The fast path is deliberately skipped for `RANGE` frames, bounded or following
frames, row exclusions other than `NO OTHERS`, custom functions, window-valued
arguments, and non-aggregate window functions. Those shapes continue using the
existing evaluator. NULL arguments retain the existing behavior: they do not
contribute to numeric aggregates, and an all-NULL frame returns NULL.

## Verification

The focused test covers partition resets, ascending ordering, all four supported
aggregate names, and NULL values:

```sh
make test-c225-incremental-window
make test-c225-window-suite
make race-c225-incremental-window
make vet-c225-incremental-window
```

The full package and race targets still expose the pre-existing M-U05
arrangement-checkpoint failures; the C225 and existing window tests pass.

## Measurement

Five `-benchmem` samples ran on Linux amd64 with an AMD Ryzen 9 5950X. The
workload evaluates four repeated running aggregates over 2,000 rows partitioned
into eight buckets.

| Path | Raw ns/op samples | Median ns/op | B/op | Allocs/op | Relative |
| --- | --- | ---: | ---: | ---: | --- |
| Existing frame rescans | 59,844,612; 65,599,333; 62,008,065; 63,728,981; 65,855,024 | 63,728,981 | 24,467,225 | 96,111 | 1.00x |
| Incremental frame state | 6,236,883; 6,117,514; 5,598,120; 5,552,759; 5,709,535 | 5,709,535 | 4,333,787 | 39,907 | 11.16x faster, 5.65x lower heap, 2.41x fewer allocations |

Run the repeatable measurement with:

```sh
make benchmark-c225-baseline
```
