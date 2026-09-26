# M038 Differential Flat-Map Small-Batch Fast Path

## Change

`FlatMapDifferentialRows` now handles exactly one input row without building
the general intermediate batch when the callback returns zero or one output.
The one-output case returns the inherited timestamp and signed weight directly;
the zero-output case returns nil without consolidation. One-input multi-output
and all multi-input calls retain the existing consolidation path, so duplicate
output identities and signed multiplicity remain unchanged.

## Benchmark

Machine: AMD Ryzen 9 5950X 16-Core Processor, Linux amd64.

Each case was run five times by the repository benchmark target. The table
uses the median of the five samples; raw samples are retained below.

| Case | Before | After | Improvement |
| --- | ---: | ---: | ---: |
| single input, one output, ns/op | 614.6 | 514.7 | 1.19x faster |
| single input, one output, B/op | 1,128 | 1,080 | 1.04x lower |
| single input, one output, allocs/op | 9 | 8 | 1 fewer allocation |
| single input, empty output, ns/op | 203.0 | 166.4 | 1.22x faster |
| single input, empty output, B/op | 384 | 336 | 1.14x lower |
| single input, empty output, allocs/op | 3 | 2 | 1 fewer allocation |
| single zero-diff input, ns/op | 29.30 | 2.342 | 12.5x faster |
| single zero-diff input, B/op | 48 | 0 | 48 B eliminated |
| single zero-diff input, allocs/op | 1 | 0 | 1 allocation eliminated |

### Raw samples

| Case | Before samples | After samples |
| --- | --- | --- |
| single input, one output, ns/op | 628.1, 639.1, 614.6, 614.0, 609.9 | 526.4, 507.3, 514.7, 509.6, 517.8 |
| single input, empty output, ns/op | 200.8, 203.0, 200.5, 207.7, 203.4 | 171.2, 166.4, 162.5, 164.0, 168.5 |
| single zero-diff input, ns/op | 28.92, 29.30, 29.36, 29.12, 29.45 | 2.354, 2.342, 2.202, 2.363, 2.254 |

The before and after runs used the same benchmark cases and five-sample
configuration. Multi-output and multi-input fallback behavior is covered by
the correctness tests but is intentionally not represented by this microbench.

## Verification

- Focused correctness tests passed before the implementation change.
- Focused correctness tests passed after the implementation change.
- `go test -race ./hat/hatSql` passed.
- Full `go test ./hat/hatSql` passed.
- Tests cover signed weights, timestamps, zero weights, callback errors,
  input/output ownership, duplicate consolidation, and multi-output fallback.
