# CH-U47 Parallel CSV Parsing

`ParseCSVParallel` and `ExternalTables.ImportCSVParallel` provide an explicit
parallel path for RFC 4180 CSV that must be materialized as rows.

```go
columns, rows, err := hatSql.ParseCSVParallel(data, hatSql.ExternalImportOptions{}, 4)
if err != nil {
	return err
}

err = tables.ImportCSVParallel("events", data, hatSql.ExternalImportOptions{}, 4)
```

The parser preserves record order, supports quoted commas and newlines, uses a
bounded worker pool, and returns the lowest malformed source record
deterministically. `ImportCSVParallel` registers the table only after every
record succeeds, so a failed import leaves the previous table unchanged.

The existing `StreamCSV` and `ImportCSV` APIs are unchanged and remain the
default choice when streaming and minimum memory usage matter. The parallel
API is opt-in; a non-positive worker count uses `GOMAXPROCS`.

## Benchmark

Host: Linux, amd64, AMD Ryzen 9 5950X 16-Core Processor.

Workload: 20,000 CSV data rows, three columns, quoted comma in every note,
four workers, five benchmark samples. The materialized serial baseline uses
the existing `StreamCSV` parser and builds the same `Row` maps as the parallel
path. The callback-only serial baseline is shown separately and is not an
apples-to-apples comparison because it does not allocate row maps.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative wall time | Relative bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Serial callback-only reference | 1,912,213 | 804,746 | 20,019 | 0.26x | 0.09x |
| Serial materialized baseline | 7,421,772 | 8,648,666 | 120,022 | 1.00x | 1.00x |
| Parallel materialized, 4 workers | 5,234,018 | 9,159,111 | 120,081 | 0.71x | 1.06x |

The parallel materialized path is 1.42x faster in wall-clock time for this
workload, with 5.9% more allocated bytes and 59 more allocations per
operation. It is not a replacement for streaming: it retains all rows and
has a small boundary-scan and worker-state overhead.

Raw five-sample results:

```text
Serial materialized:
7870507 ns/op 8648741 B/op 120022 allocs/op
7421772 ns/op 8648664 B/op 120022 allocs/op
7304032 ns/op 8648696 B/op 120022 allocs/op
7502804 ns/op 8648666 B/op 120022 allocs/op
7384098 ns/op 8648662 B/op 120022 allocs/op

Parallel, 4 workers:
4939628 ns/op 9159238 B/op 120081 allocs/op
5332276 ns/op 9159245 B/op 120081 allocs/op
5132447 ns/op 9159111 B/op 120081 allocs/op
5234018 ns/op 9159063 B/op 120081 allocs/op
5289831 ns/op 9159047 B/op 120081 allocs/op
```

Benchmark command:

```text
make codex-chu47-benchmark
```
