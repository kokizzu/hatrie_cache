# C228: Stable External Sort Runs

## Status

C228 is verified in the existing external sort implementation. This progress
commit adds regression coverage and documentation; it does not change the
production sort path because the required stable ordering was already present.

## Behavior

When external `ORDER BY` spills rows into multiple runs, equal ordering keys
retain their input order. The source ordinal is carried in `sqlSpillOutput`,
serialized with each temporary run, and used as the final comparator key during
initial run sorting and every merge pass. This keeps the result independent of
run boundaries and merge fan-in.

The invariant is checked with 192 external rows, three repeated sort keys, a
128-byte sort threshold, and spill cleanup assertions:

```sh
make test-c228-external-sort
```

## Measurement

The focused benchmark forces the same 128-byte threshold and five independent
samples on Linux amd64, AMD Ryzen 9 5950X:

| Path | Raw ns/op samples | Median ns/op | B/op | Allocs/op |
| --- | --- | ---: | ---: | ---: |
| Stable external sort, 192 rows | 9,210,662; 9,290,696; 9,204,048; 9,039,272; 9,065,712 | 9,204,048 | 1,655,386 | 31,047 |

Run it with:

```sh
make benchmark-c228-external-sort
```

There is no before/after production delta to report: no algorithm or data
format changed for C228. The ordinal is a small per-record correctness cost,
but removing it would make equal-key output depend on spill-run boundaries.
