# CH-G02 Bounded Grace-Hash Join

CH-G02 is already implemented as the executor's streamed partitioned hash
join. It is a resource-bound path, not a raw-speed optimization.

## Configuration

```go
result, err := hatSql.ExecuteSQLQueryContext(ctx, query, resolver, hatSql.SQLQueryOptions{
	MaxRows:        100000,
	MaxJoinBytes:   4 << 10,
	SpillDirectory: "/var/lib/hatrie/spill",
	MaxSpillBytes:  64 << 20,
})
```

For an eligible direct inner equality join, `MaxJoinBytes` causes the executor
to stream both inputs into bounded hash partitions. Partition pairs are read,
joined, and removed as they complete. `MaxSpillBytes` is a hard disk budget;
failed or canceled queries remove their temporary files. The default remains
the existing in-memory path because `MaxJoinBytes=0` disables spill.

`JoinOverflowPolicy` can make the intended behavior explicit. `spill` requires
the spill settings; `reject` fails when materialized join input exceeds the
configured byte bound. The spill path also contributes to the optional
query-wide `MaxQuerySpillBytes` quota.

## Correctness coverage

Existing tests verify duplicate-key output, streamed resolver usage, the
partitioned `EXPLAIN ANALYZE` node, disk-budget failure, and cleanup after
success or failure. The path is only selected for supported inner equality
joins; other join shapes retain their existing executor.

## Measurement

Workload: two 4,096-row `CACHE` sources with unique `int64` equality keys and
the same resolver implementation. Five samples used `-benchtime=2s` and
`-benchmem` on the repository's AMD Ryzen 9 5950X host.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| In-memory hash join | 6,666,469 | 9,817,394 | 49,230 | Reference |
| Bounded grace-hash spill | 90,904,394 | 22,165,976 | 450,414 | 13.64x slower; 2.26x cumulative bytes; 9.15x allocations |

The spill path is intentionally slower because it encodes, writes, reads, and
deletes partition files. `B/op` is cumulative allocation, not peak retained
memory; the benefit is that the join can complete under a configured memory
bound instead of failing or retaining the full hash table. It should not be
enabled for latency-sensitive joins unless bounded resource usage is the
primary requirement.

Raw samples:

```text
BenchmarkCH002HashJoinBaseline
6935902 ns/op 9817401 B/op 49230 allocs/op
6665742 ns/op 9817394 B/op 49230 allocs/op
6768044 ns/op 9817397 B/op 49230 allocs/op
6666469 ns/op 9817383 B/op 49230 allocs/op
6605016 ns/op 9817382 B/op 49230 allocs/op

BenchmarkCH002GraceHashJoin
103697319 ns/op 22166123 B/op 450415 allocs/op
86559401 ns/op 22165892 B/op 450415 allocs/op
122301348 ns/op 22165976 B/op 450421 allocs/op
84855765 ns/op 22166124 B/op 450414 allocs/op
90904394 ns/op 22165656 B/op 450414 allocs/op
```

Run the repeatable checks with:

```text
make m242-ch-g02-test
make m242-ch-g02-benchmark
```
