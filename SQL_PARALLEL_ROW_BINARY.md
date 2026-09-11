# Parallel RowBinary Decode

`hatSql.DecodeSQLRowBinary` now uses a ClickHouse-style independent-block idea
for large RowBinary payloads. The existing row-oriented wire format is first
scanned with the allocation-free `skipSQLRowBinaryValue` helpers to record row
boundaries. Disjoint row ranges are then decoded concurrently into fixed
positions in the result slice, preserving input order and byte-copy semantics.

The automatic path is deliberately conservative:

- payloads below 64 KiB stay on the serial decoder;
- fewer than 256 rows stay serial after boundary indexing;
- a process with one logical processor stays serial;
- schema validation, nullable markers, type validation, row limits, and
  malformed-input rejection remain unchanged.

Callers that want the explicit path can use
`hatSql.DecodeSQLRowBinaryParallel`. It has the same fallback rules and wire
format as `DecodeSQLRowBinary`.

The offset index is bounded to one integer per decoded row and is released
with the decode call. It adds no wire or persistence bytes. Large payloads pay
about 1.27% more transient bytes and 10 more allocations in the benchmark
below, in exchange for a 2.16x CPU improvement on eight logical workers.

## Verification

The regression tests compare parallel and serial results over fixed-width,
variable-width, nullable, boolean, and byte columns, verify row order and byte
copying, and reject truncated or trailing input:

```sh
make test-m065u-parallel-row-binary
```

Package, race, vet, and documentation checks use the corresponding `m065u`
Makefile targets.

## Measurement

Seven `250ms` samples were collected on `linux/amd64`, AMD Ryzen 9 5950X,
with `GOMAXPROCS=8`. The workload decodes a 4,096-row RowBinary payload with
`int64`, string, boolean, and nullable byte columns. The baseline is the same
benchmark copied into a detached worktree at the parent revision.

| Metric | Serial baseline | Automatic parallel path | Change |
| --- | ---: | ---: | ---: |
| Median CPU time | 1,222,877 ns/op | 565,288 ns/op | 2.16x faster |
| Median throughput | 203.62 MB/s | 440.49 MB/s | 2.16x higher |
| Median allocated bytes | 1,961,439 B/op | 1,986,362 B/op | 1.27% higher |
| Median allocations | 26,792 allocs/op | 26,802 allocs/op | 10 higher |

Raw output is recorded in [BENCHMARK.md](BENCHMARK.md#ch-047-parallel-rowbinary-decode).
