# M038: Differential Flat-Map Two-Output Fast Paths

## Change

`FlatMapDifferentialRows` now handles the common one-input/two-output case without
expanding the input through the general consolidation map:

- two distinct output keys are returned directly, preserving input time and diff;
- two equal output keys are combined directly with checked diff arithmetic and the
  first row payload is retained;
- empty keys and diff overflow keep the existing errors;
- zero-weight output behavior, more than two outputs, and multi-input expansion
  continue through the existing consolidation path.

Both direct paths clone output rows, so callers cannot mutate the source update by
mutating a returned row.

## Benchmark

Command:

```text
make benchmark-codex-m038-flatmap-distinct
```

The benchmark uses one input row and a callback that emits two rows. Each value is
the median of five runs from the same benchmark harness and CPU.

| Case | Baseline ns/op | After ns/op | Speedup | Baseline B/op | After B/op | Baseline allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| two distinct keys | 1,102 | 820.4 | 1.34x | 1,888 | 1,808 | 13 | 12 |
| two equal keys | 1,031 | 642.3 | 1.60x | 1,888 | 1,440 | 13 | 10 |

### Raw baseline samples

| Case | ns/op samples | B/op | allocs/op |
| --- | --- | ---: | ---: |
| two distinct keys | 1,115; 1,112; 1,064; 1,102; 1,031 | 1,888 | 13 |
| two equal keys | 1,034; 1,026; 1,016; 1,067; 1,031 | 1,888 | 13 |

### Raw final samples

| Case | ns/op samples | B/op | allocs/op |
| --- | --- | ---: | ---: |
| two distinct keys | 867.8; 816.1; 796.1; 833.6; 797.8 | 1,808 | 12 |
| two equal keys | 626.5; 636.6; 653.3; 662.6; 674.3 | 1,440 | 10 |

## Verification

- focused behavior tests pass, including row ownership, output order, signed
  weights, duplicate consolidation, and overflow;
- focused race test passes;
- full `hat/hatSql` package tests pass;
- no fallback behavior was changed for larger or multi-input batches.
