# HLL Aggregate Envelope View

## Change

`NewHyperLogLogFromAggregateState` now uses an internal validated envelope
decoder that borrows the wire payload while it is being decoded. The generic
`UnmarshalAggregateStateEnvelope` API still copies its payload, so callers keep
the existing ownership guarantee. HLL copies its register bytes once into the
returned value.

The wire format and validation rules are unchanged.

## Benchmark

Machine: AMD Ryzen 9 5950X, Linux amd64. Each row is the median of five
benchmark samples. The baseline is the exact parent branch, and both versions
use the same benchmark workload.

| Precision | Baseline ns/op | After ns/op | Speedup | Baseline B/op | After B/op | Bytes reduction | Baseline allocs/op | After allocs/op |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10 | 1,917 | 1,567 | 1.22x | 2,192 | 1,040 | 2.11x | 3 | 2 |
| 14 | 25,670 | 23,179 | 1.11x | 34,832 | 16,400 | 2.12x | 3 | 2 |
| 16 | 101,369 | 96,194 | 1.05x | 139,280 | 65,552 | 2.12x | 3 | 2 |

## Verification

- Allocation regression test: passed with a two-allocation budget.
- Full `hat/hatDataStructure` tests: passed.
- Race tests: passed.
- `go vet`: passed.
- Generic envelope copy behavior remains covered by the existing envelope tests.
