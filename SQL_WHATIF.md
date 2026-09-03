# SQL What-If Index Advisor

`hatSql.ExplainSQLWhatIf` estimates whether one proposed index is worth
building. It is read-only: it does not create an index, change the query
planner, retain query state, or change normal SQL results.

```go
report, err := hatSql.ExplainSQLWhatIf(ctx, hatSql.SQLWhatIfRequest{
	Query: "SELECT id FROM CACHE('events') WHERE region = 'apac'",
	Index: hatSql.SQLWhatIfIndex{
		Kind:   hatSql.SQLWhatIfIndexEquality,
		Fields: []string{"region"},
	},
}, resolver)
if err != nil {
	return err
}
fmt.Printf("read %d -> %d rows; recommendation=%s\n",
	report.BaselineRowsRead, report.HypotheticalRowsRead, report.Recommendation)
```

## Supported Shapes

The first implementation accepts one direct `CACHE` source and simple scalar
fields:

| Kind | Query shape | Measured quantity |
| --- | --- | --- |
| `SQLWhatIfIndexEquality` | `field = literal`, including `AND` conjunctions | Estimated rows and bytes avoided by equality postings |
| `SQLWhatIfIndexRange` | One numeric `<`, `<=`, `>`, or `>=` predicate | Estimated rows and bytes avoided by an ordered range index |
| `SQLWhatIfIndexOrder` | Simple `ORDER BY` fields | Whether the candidate can eliminate the sort |
| `SQLWhatIfIndexGroup` | Simple `GROUP BY` fields | Whether the candidate can provide grouping order for a future streaming aggregate |

`Source` defaults to the query's `CACHE` key. Composite equality fields may be
listed in any order when all predicate fields are covered. ORDER and GROUP
fields must be an index prefix in the same order as the query.

Joins, CTEs, set operations, expressions, subqueries, and non-scalar
predicates return an unsupported report or a validation error. An unsupported
report is safe to display in tooling because `Supported` and `Notes` explain
why no recommendation was made.

## Statistics And Cost

If the resolver implements `SQLWhatIfSourceStatisticsResolver`, the advisor
uses bounded source statistics and does not read source rows. Equality
estimates use distinct-value counts; numeric range estimates use the supplied
minimum and maximum. Existing JSON index metadata is recognized through
`JSONIndexStatsResolver`.

Without statistics, the advisor reads the source once and computes exact
matching row counts. This is intentionally explicit and outside the normal
query path. It is useful for small sources and test fixtures, but production
tuning tools should expose the statistics-backed path where possible.

The report includes:

- baseline and hypothetical rows/bytes read;
- rows/bytes skipped;
- estimated retained index bytes;
- estimated index bytes maintained per mutation;
- whether a matching index already exists;
- `build`, `keep existing`, `skip`, `insufficient workload`, or `unsupported`.

Index-size values are planning estimates, not allocator measurements. They
must be validated with the concrete index implementation before deployment.

## Safety And Compatibility

The function requires a caller-provided resolver, validates the query before
reading it, honors context cancellation, and never evaluates a proposed index
as an authority for returned data. It does not expose source rows in the
report. Existing synchronous SQL, defaults, wire formats, and storage formats
are unchanged.

## Verification And Benchmark

```sh
make test-sql-whatif
make benchmark-sql-whatif
```

On an AMD Ryzen 9 5950X, the 10,000-row exact fallback benchmark measured a
median of about `6.36 ms`, `2.7 MB`, and `80,017` allocations per report. The
same 100,000-row numeric estimate with source statistics measured about
`2.29 us`, `3.5 KB`, and `19` allocations. The statistics path is therefore the
preferred operational path; the fallback trades memory and CPU for exact
observations.

The design follows the intent of ClickHouse hypothetical indexes and
`EXPLAIN WHATIF`, and the structured plan/filter explanations documented by
[ClickHouse](https://clickhouse.com/clickhouse/feature-journey) and
[Materialize](https://materialize.com/docs/sql/explain-filter-pushdown/).
