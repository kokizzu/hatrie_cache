# CHU66: Grouped Numeric Aggregate Kernel

CHU66 specializes the dictionary-group SQL executor for numeric aggregate
fields stored in validated packed `int64` or `float64` columns. Each aggregate
state reads the packed word and validity bitmap directly instead of calling the
generic `ColumnarBatch.Value` accessor for every row.

The optimization is intentionally narrow. It is selected for the
dictionary-group path, which includes ordered grouped queries such as:

```sql
SELECT group,
       SUM(value) AS total,
       AVG(value) AS average,
       MIN(value) AS minimum,
       MAX(value) AS maximum
FROM CACHE('items')
GROUP BY group
ORDER BY group;
```

Plain columns, dictionary values, missing fields, malformed packed layouts,
unsupported expressions, and NULL values retain the existing behavior. NULL
rows are skipped exactly as before. A simple unordered `GROUP BY` may be
handled earlier by the general vector-group executor; CHU66 does not claim that
path.

## Benchmark

The control is the round24 implementation with the same query, 100,000 rows,
64 dictionary groups, four numeric aggregates, and 20% NULL values. Five
`go test -bench` samples ran on Linux/amd64 with an AMD Ryzen 9 5950X.

| Version | Median ns/op | Median B/op | Median allocs/op | CPU improvement | Heap improvement | Allocation improvement |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Round24 control | 24,313,868 | 2,642,677 | 320,556 | 1.00x | 1.00x | 1.00x |
| CHU66 | 9,238,571 | 82,589 | 553 | 2.63x faster | 32.00x less | 579.67x fewer |

Raw samples (`ns/op / B/op / allocs/op`):

```text
control: 24286863/2643534/320559, 24313868/2642677/320556, 24307231/2642675/320556, 24484933/2642676/320556, 24566246/2642680/320556
CHU66:    9253236/82588/553,       9258455/82631/553,       9238571/82588/553,       9227202/82589/553,       9221945/82589/553
```

## Verification

```text
make format-chu66-grouped-numeric-aggregate
make test-chu66-grouped-numeric-aggregate
make race-chu66-grouped-numeric-aggregate
make vet-chu66-grouped-numeric-aggregate
make test-chu64-count-field
make test-chu65-typed-numeric-aggregate
```

The full `hatSql` package still reports the two pre-existing typed-table
arrangement checkpoint failures; no CHU66-specific failure was observed.
