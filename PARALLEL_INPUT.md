# Parallel NDJSON Input

`hatSql.ParseNDJSONParallel(data, workers)` parses one JSON object per
non-empty line with a bounded worker pool. It preserves source order in the
returned rows. `workers <= 0` uses `runtime.GOMAXPROCS(0)`; callers can pass
`1` for a deterministic single-worker path.

`ExternalTables.ImportNDJSONParallel` uses the parser and replaces the named
external-table snapshot only after every line succeeds. A malformed record or
the JSON value `null` returns an error and leaves the previous snapshot
unchanged. If several records are invalid, the error for the lowest
one-based source line is returned, including blank lines in line numbering.

The existing `ImportNDJSON` method remains sequential and unchanged. The
parallel method is opt-in because worker coordination and result bookkeeping
cost more memory on small inputs.

## Local Benchmark

This was measured with `BenchmarkExternalTablesImportNDJSONParallel` using the
same 10,000-row payload and four workers on an AMD Ryzen 9 5950X:

| Path | Time per import | Memory per import | Allocations per import | Relative time |
| --- | ---: | ---: | ---: | ---: |
| Sequential | 13.25 ms | 9.61 MB | 150,005 | 1.00x |
| Parallel, 4 workers | 7.41 ms | 9.86 MB | 150,016 | 0.56x |

The parallel path was approximately 1.79x faster, used 2.6% more memory, and
used 11 more allocations in this run. Results depend on payload size, JSON
shape, worker count, and available CPU. For small or latency-sensitive
imports, the existing sequential method can avoid that fixed coordination
cost.
