# CH-052 Prepared Temporal Expressions

This is a ClickHouse-inspired expression-preparation optimization. Immutable
IANA time-zone literals in `PARSE_TIMESTAMP` and `AT TIME ZONE` are resolved
when the SQL expression is bound instead of loading the zone data for every
row. Fixed two-argument temporal functions also evaluate their arguments
directly, avoiding a temporary argument slice on the hot path.

## Scope

- `PARSE_TIMESTAMP(value, 'IANA/Zone')` caches the bound `*time.Location`.
- `timestamp AT TIME ZONE 'IANA/Zone'` caches the bound `*time.Location`.
- Invalid literal zones remain evaluation errors with the original SQL token.
- Parameter-bound zones are prepared after parameter binding.
- Dynamic zone expressions continue to load and validate the zone at runtime.
- The optimization is per prepared expression; there is no unbounded global
  time-zone cache and no storage, wire, or configuration change.

The retained cost is one small immutable program per prepared expression. The
zone-loading work moves from every row to query preparation/binding.

## Measurement

Samples were collected on Linux `amd64`, Go benchmark workers `-32`, an AMD
Ryzen 9 5950X, and five runs per case. The benchmark evaluates the same bound
expression repeatedly and reports CPU, heap bytes, and allocation count.

| Workload | Before ns/op | After ns/op | CPU improvement | Before B/op | After B/op | Byte improvement | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `PARSE_TIMESTAMP` with literal zone | 5,801 | 435.4 | 13.3x | 1,200 | 144 | 8.3x | 19 | 4 |
| `PARSE_TIMESTAMP` with dynamic zone | 6,416 | 6,661 | 0.96x | 1,200 | 1,200 | 1.00x | 19 | 19 |
| `AT TIME ZONE` with row timestamp | 5,531 | 94.31 | 58.7x | 1,088 | 24 | 45.3x | 16 | 1 |
| `AT TIME ZONE` with timestamp literal | 5,760 | 78.77 | 73.1x | 1,088 | 24 | 45.3x | 16 | 1 |
| `TIMESTAMP_ADD` with row timestamp | 140.5 | 135.7 | 1.04x | 24 | 24 | 1.00x | 1 | 1 |

The dynamic-zone result is intentionally not presented as a speedup; its
small median movement is within ordinary benchmark noise and its allocations
and bytes are unchanged. Fully constant `PARSE_TIMESTAMP`, `TIMESTAMP_ADD`,
and `TIMESTAMP_DIFF` expressions were already folded by the parser at about
8 ns/op with zero allocations, so this change does not duplicate that work.

## Raw output

```text
# Before
BenchmarkCH052TemporalLiteralEvaluation/parse_literal_zone-32  201634  5857 ns/op  1200 B/op  19 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/parse_literal_zone-32  209763  5731 ns/op  1200 B/op  19 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/parse_literal_zone-32  202287  5801 ns/op  1200 B/op  19 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/parse_literal_zone-32  205314  5760 ns/op  1200 B/op  19 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/parse_literal_zone-32  211828  5925 ns/op  1200 B/op  19 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/parse_dynamic_zone-32  179358  6422 ns/op  1200 B/op  19 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/parse_dynamic_zone-32  187674  6326 ns/op  1200 B/op  19 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/parse_dynamic_zone-32  180022  6444 ns/op  1200 B/op  19 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/parse_dynamic_zone-32  183516  6416 ns/op  1200 B/op  19 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/parse_dynamic_zone-32  185950  6348 ns/op  1200 B/op  19 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/timezone_literal-32    201151  5776 ns/op  1088 B/op  16 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/timezone_literal-32    188666  5955 ns/op  1088 B/op  16 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/timezone_literal-32    224335  5531 ns/op  1088 B/op  16 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/timezone_literal-32    215046  5385 ns/op  1088 B/op  16 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/timezone_literal-32    213396  5366 ns/op  1088 B/op  16 allocs/op

# After
BenchmarkCH052TemporalLiteralEvaluation/parse_literal_zone-32  2882800  435.4 ns/op  144 B/op  4 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/parse_literal_zone-32  2760472  426.2 ns/op  144 B/op  4 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/parse_literal_zone-32  2877477  418.8 ns/op  144 B/op  4 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/parse_literal_zone-32  2865595  438.3 ns/op  144 B/op  4 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/parse_literal_zone-32  2749321  444.1 ns/op  144 B/op  4 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/parse_dynamic_zone-32  188316  6274 ns/op  1200 B/op  19 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/parse_dynamic_zone-32  198466  6395 ns/op  1200 B/op  19 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/parse_dynamic_zone-32  182400  6665 ns/op  1200 B/op  19 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/parse_dynamic_zone-32  175747  6661 ns/op  1200 B/op  19 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/parse_dynamic_zone-32  180006  6670 ns/op  1200 B/op  19 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/timezone_literal-32    11966803  94.31 ns/op  24 B/op  1 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/timezone_literal-32    11688801  98.41 ns/op  24 B/op  1 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/timezone_literal-32    11087845  99.39 ns/op  24 B/op  1 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/timezone_literal-32    12468076  93.42 ns/op  24 B/op  1 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/timezone_literal-32    13696057  88.31 ns/op  24 B/op  1 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/timezone_all_literals-32  15162384  79.86 ns/op  24 B/op  1 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/timezone_all_literals-32  15071505  79.55 ns/op  24 B/op  1 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/timezone_all_literals-32  15211076  77.87 ns/op  24 B/op  1 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/timezone_all_literals-32  14387828  78.77 ns/op  24 B/op  1 allocs/op
BenchmarkCH052TemporalLiteralEvaluation/timezone_all_literals-32  15588205  78.19 ns/op  24 B/op  1 allocs/op
```

## Verification

```text
make test-ch052-temporal-program
make benchmark-ch052-temporal-program
make format-ch052-temporal-program
make race-ch052-temporal-program
make vet-ch052-temporal-program
make test-sql-extensions
```
