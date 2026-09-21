# CH-047: Parallel CSV Format Parsing

`hatSql.ParseCSVParallel(reader, workers)` and
`ExternalTables.ImportCSVParallel(name, data, workers)` are opt-in bulk APIs.
They use a bounded serial framing pass to find complete RFC 4180 records, so
quoted newlines and escaped quotes never split a worker block. Each block is
decoded with the standard library CSV parser, then rows are returned and
published in input order. The first failing block is reported deterministically.

The existing `StreamCSV`, `ImportCSVReader`, and `ImportCSV` paths are
unchanged and remain the default. They are preferable for streaming input,
strict row/byte limits, or workloads where the additional retained input and
framing metadata are more costly than parallel decode.

## Measurement

Command: `make benchmark-ch047-csv` on an AMD Ryzen 9 5950X, Go `amd64`.
Three samples per benchmark used the same 20,000-row CSV fixture, including
quoted multiline fields. The parallel benchmarks used four workers.

| Path | Median ns/op | B/op | allocs/op | Relative CPU | Relative bytes | Relative allocations |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Serial parse | 9,779,085 | 11,649,621 | 140,040 | 1.00x | 1.00x | 1.00x |
| Parallel parse | 6,289,099 | 11,506,302 | 120,131 | **1.56x faster** | 0.99x | 0.86x |
| Serial import | 11,283,701 | 9,008,193 | 120,047 | 1.00x | 1.00x | 1.00x |
| Parallel import | 8,363,252 | 11,507,130 | 120,137 | **1.35x faster** | **1.28x** | 1.00x |

The import path trades about 28% more allocated bytes for about 35% lower
latency. This is why the parallel APIs are explicit and do not replace the
streaming defaults.

### Raw Samples

```text
BenchmarkCH047CSVSerialImport:   10398100 11521763 11283701 ns/op; 9008238 9008190 9008193 B/op; 120047 allocs/op
BenchmarkCH047CSVParallelImport:   8363252  9401823  7833772 ns/op;11507386 11507130 11506946 B/op; 120137  allocs/op
BenchmarkCH047CSVSerialParse:      9779085  9670212 10028238 ns/op;11649617 11649625 11649621 B/op; 140039  allocs/op
BenchmarkCH047CSVParallelParse:    5370867  6289099  7079717 ns/op;11506263 11506302 11506401 B/op; 120131  allocs/op
```

Correctness and race coverage:

```text
make test-ch047-csv
make race-ch047-csv
```

These cover quoted newlines, escaped quotes, CRLF, blank records, header-only
input, deterministic parse errors, row order, and atomic replacement after a
failed import.
