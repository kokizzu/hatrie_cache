# M038: Differential Filter Zero-Weight Fast Path

## Change

`FilterDifferentialRows` now returns immediately for a single non-empty-key
update whose differential weight is zero. The callback is still not invoked,
matching the existing loop, and invalid empty keys still return the existing
key error. Multi-row batches and non-zero single rows retain the original
filter loop.

The broader one-row callback fast path was measured and discarded because it
introduced a small selected-row latency regression. Only the zero-weight
allocation win is retained.

## Benchmark

Command:

```text
make benchmark-codex-m038-filter
```

The benchmark runs the exact pre-change loop (`legacy`) and the implementation
under test (`optimized`) in the same process, five samples per case.

| Case | Legacy median | Optimized median | Speedup | Legacy B/op | Optimized B/op | Legacy allocs/op | Optimized allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| selected | 215.2 ns | 204.7 ns | 1.05x | 384 | 384 | 3 | 3 |
| rejected | 212.1 ns | 206.7 ns | 1.03x | 384 | 384 | 3 | 3 |
| zero weight | 29.59 ns | 3.256 ns | 9.09x | 48 | 0 | 1 | 0 |

### Raw samples

| Case | Legacy ns/op | Optimized ns/op |
| --- | --- | --- |
| selected | 218.7; 210.6; 219.7; 204.1; 215.2 | 211.2; 210.0; 204.7; 202.0; 201.8 |
| rejected | 212.1; 206.9; 203.5; 219.1; 216.2 | 211.8; 206.7; 203.9; 200.4; 207.2 |
| zero weight | 29.59; 29.70; 30.52; 29.10; 29.43 | 3.381; 3.253; 3.307; 3.256; 3.066 |

## Verification

- focused tests cover selected rows, rejected rows, zero-weight callback
  suppression, row ownership, missing keys, and callback errors;
- focused race verification passes;
- full `hat/hatSql` package verification passes;
- the broader selected/rejected callback fast path was not retained after
  measuring its tradeoff.
