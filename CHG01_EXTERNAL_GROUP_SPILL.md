# CH-G01: Bounded External `GROUP BY` Aggregation Spill

Hatrie Cache can spill supported grouped aggregation state to disk when the
in-memory group budget is reached. This is an opt-in protection for
high-cardinality queries: it lets a query complete under a bounded memory
budget instead of rejecting the query because its groups do not fit.

## Scope

The current path is deliberately conservative. It applies to streamable
`CACHE` and `VALUES` sources with:

- one direct `GROUP BY` field;
- direct `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX` projections supported by the
  existing mergeable aggregate path; and
- either no `ORDER BY`, or one `ORDER BY` on the same group field.

Queries with multiple group fields, `HAVING`, joins, CTEs, expressions that are
not in the direct aggregate subset, or incompatible ordering retain their
existing executor and fallback behavior. No-order SQL results remain
semantically unordered; the spill merge emits group keys in its deterministic
merge order only as an implementation detail.

## Configuration

Set these fields on the existing SQL execution options:

```go
options := hatSql.SQLQueryOptions{
	MaxGroupBytes:  16 << 10,
	SpillDirectory: "/var/lib/hatrie-cache/sql-spill",
	MaxSpillBytes:  64 << 20,
}
```

All three controls are opt-in. Leaving `MaxGroupBytes` at its existing zero
value keeps the normal in-memory executor. `SpillDirectory` selects where
temporary runs are written, and `MaxSpillBytes` bounds aggregate spill disk
usage. The query fails once the disk budget is exhausted; it does not silently
continue without a bound.

Spill files can contain group keys and aggregate values. Use a directory with
permissions appropriate for the query data. Existing SQL spill protection
settings, when configured by the caller, continue to apply. Successful and
failed executions remove their temporary run files; cleanup errors are
reported rather than ignored.

## Correctness Coverage

The regression tests cover:

- duplicate groups and exact aggregate results for an unordered `VALUES`
  query;
- the same result through a streaming `CACHE` resolver;
- disk-budget failure and cleanup of all temporary spill files; and
- the pre-existing ordered spill path and aggregate fallback coverage.

Run the focused checks with:

```text
make test-chg01
make race-chg01
make vet-chg01
```

## Measurement

The benchmark uses 2,048 unique groups and a 16 KiB group-memory budget. The
unbounded baseline is the same query with the normal in-memory executor. The
bounded baseline is also run against the pre-feature source: it cannot support
the workload and reports `supported=0`, because it rejects the query when the
group budget is exceeded.

| Path | Median ns/op | Median B/op | Median allocs/op | Supported |
| --- | ---: | ---: | ---: | ---: |
| Before, unbounded in-memory | 3,943,443 | 4,622,428 | 32,847 | 1.0 |
| Before, bounded spill request | 2,548,013 | 2,316,579 | 20,562 | 0.0 |
| After, unbounded in-memory | 3,992,577 | 4,622,961 | 32,847 | 1.0 |
| After, bounded external spill | 13,702,265 | 5,871,345 | 68,110 | 1.0 |

The feature therefore changes availability under the configured memory cap,
not the fast path’s performance: unbounded execution is within normal run
noise, while a successful bounded spill is about 3.4x slower, uses about 1.27x
the allocated bytes, and performs about 2.07x as many allocations as the
unbounded run. That is the expected CPU, allocation, and disk-I/O tradeoff for
avoiding an unbounded in-memory group table. It is not enabled by default.

Reproduce the paired raw runs with:

```text
make prepare-chg01-benchmark-baseline
make benchmark-chg01-before
make benchmark-chg01-after
make print-chg01-benchmark
```

The complete raw samples are retained in the [CH-G01 section of
`BENCHMARK.md`](BENCHMARK.md#ch-g01-bounded-external-group-by-aggregation-spill).
