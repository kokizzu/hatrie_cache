# TT-024 Materialized Text Index Auto-Selection

`MaterializedSource` already supported a positional token index, but the SQL
adapter did not expose that index to the ordinary `CONTAINS(field, literal)`
planner path. Queries through `SQLResolverAdapter` therefore scanned every
row even after `BuildTextIndex` had been called.

The completed path is deliberately narrow:

- `SQLResolverAdapter.ResolveSQLTextSource` forwards `CONTAINS` lookups to
  the source's existing text index.
- The resolver tokenizes the literal, starts from the smallest posting list,
  and intersects sorted row IDs for the remaining tokens.
- The normal SQL evaluator still rechecks `CONTAINS` on the candidates, so
  posting-list false positives cannot change query results.
- Without a built index, unsupported expressions, or a non-literal query, the
  existing full scan remains the fallback.
- The planner's index-use recorder recognizes this exact `CONTAINS` shape;
  no additional `EXPLAIN` node is added, preserving the existing plan shape.

## Verification

The focused test covers direct adapter lookup, SQL result correctness, sorted
results, and automatic index-use reporting:

```text
make test-tt024-materialized-text
make race-tt024-materialized-text
make test-tt024-text
make vet-tt024-materialized-text
```

## Benchmark

`BenchmarkTT024MaterializedTextIndexSelection` creates the same deterministic
20,000-row source for both cases and runs:

```sql
FROM CACHE('docs') AS doc
WHERE CONTAINS(doc.body, 'alpha gamma')
SELECT COUNT(*)
```

Five `-benchmem` samples were run on Linux/amd64 with an AMD Ryzen 9 5950X.
The scan returns the same count as the indexed path; only the source index
creation differs between subbenchmarks.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Full scan (`scan_baseline`) | 25,548,346 | 21,950,523 | 180,034 | Baseline |
| Automatic text index (`automatic_text_index`) | 37,894 | 21,608 | 173 | 674.2x lower CPU, 1,015.9x fewer bytes, 1,040.7x fewer allocations |

`B/op` is timed allocation volume, not retained heap size. The index keeps its
posting lists and therefore trades retained sidecar memory and write/build
maintenance for much lower query work. The existing text-index build and
retained-memory discussion remains in
[TT024_POSITIONAL_TEXT_INDEX.md](TT024_POSITIONAL_TEXT_INDEX.md).

Raw output from `make benchmark-tt024-materialized-text`:

```text
BenchmarkTT024MaterializedTextIndexSelection/scan_baseline-32          45  26053179 ns/op  21950524 B/op  180034 allocs/op
BenchmarkTT024MaterializedTextIndexSelection/scan_baseline-32          46  25355048 ns/op  21950523 B/op  180034 allocs/op
BenchmarkTT024MaterializedTextIndexSelection/scan_baseline-32          46  25460686 ns/op  21950517 B/op  180034 allocs/op
BenchmarkTT024MaterializedTextIndexSelection/scan_baseline-32          45  26164124 ns/op  21950637 B/op  180034 allocs/op
BenchmarkTT024MaterializedTextIndexSelection/scan_baseline-32          44  25548346 ns/op  21950518 B/op  180034 allocs/op
BenchmarkTT024MaterializedTextIndexSelection/automatic_text_index-32  33000     37979 ns/op     21608 B/op       173 allocs/op
BenchmarkTT024MaterializedTextIndexSelection/automatic_text_index-32  31130     36341 ns/op     21608 B/op       173 allocs/op
BenchmarkTT024MaterializedTextIndexSelection/automatic_text_index-32  33244     36510 ns/op     21608 B/op       173 allocs/op
BenchmarkTT024MaterializedTextIndexSelection/automatic_text_index-32  30375     38082 ns/op     21608 B/op       173 allocs/op
BenchmarkTT024MaterializedTextIndexSelection/automatic_text_index-32  31264     37894 ns/op     21608 B/op       173 allocs/op
```
