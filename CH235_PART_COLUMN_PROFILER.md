# C235: Part-Column Query Profiling

This adopts the ClickHouse-style idea of attributing query work to physical
parts and columns. It extends the existing opt-in `hatSql.SQLQueryProfiler`
with a bounded read/write aggregate keyed by `(query, part, column,
operation)`.

## API

- `SQLQueryProfilerOptions.MaxPartColumnsPerQuery` defaults to `64` and is
  bounded by a hard safety limit.
- `SQLQueryProfiler.RecordPartColumn` accepts `read` or `write` observations
  with CPU time, rows, and bytes.
- `PartColumnProfile` and `PartColumnProfiles` return independent,
  deterministic snapshots sorted by part, column, operation.
- Saturating counters prevent overflow from corrupting diagnostics.
- The map is allocated only after the new recording method is called. Existing
  query samples and normal SQL execution do not use it.

## Measurements

Five `-benchmem` samples ran on Linux amd64 with an AMD Ryzen 9 5950X.

| Path | Raw ns/op samples | Median ns/op | B/op | allocs/op | Relative CPU |
| --- | --- | ---: | ---: | ---: | ---: |
| Existing `Record` control, parent commit | 14.13, 15.24, 14.99, 14.69, 14.97 | 14.97 | 0 | 0 | 1.00x |
| C235 `RecordPartColumn`, one hot key | 102.6, 95.39, 99.93, 100.1, 101.7 | 100.1 | 0 | 0 | 6.69x |
| C235 `PartColumnProfile`, 64 entries | 12614, 12315, 12337, 10927, 13632 | 12337 | 5528 | 4 | no direct control |

The recording cost is intentional and opt-in; it is not added to the existing
sample path. The snapshot allocates a detached sorted result, so callers can
export or inspect it without exposing internal maps. The bound limits retained
cardinality and dropped observations are reported explicitly.

## Verification

- Red test first: `make test-ch235-part-column-profiler` failed on the missing
  API before implementation.
- Focused correctness: `make test-ch235-part-column-profiler` passes.
- Race correctness: `make race-ch235-part-column-profiler` passes.
- Package verification: `make test-ch235-package` still has the two existing
  typed-table arrangement checkpoint failures reproduced by
  `make verify-ch235-baseline`; the failures are outside this profiler path.
