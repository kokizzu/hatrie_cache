# CH003 Parallel Hash Join

CH-G03 is implemented as an opt-in parallel probe path for direct materialized
`INNER` equality hash joins.

```go
options := hatSql.SQLQueryOptions{
	JoinWorkers: 4,
}
```

`JoinWorkers` defaults to zero, so existing queries keep the sequential path.
The executor builds one typed hash index, disables its mutable adaptive Bloom
sampling state, and probes it from contiguous left-side chunks. Results are
concatenated in chunk order, preserving the established left-row and
right-row order. Unsupported shapes, including outer joins, index joins, and
non-equality joins, use the existing planner.

The option is deliberately opt-in. It adds goroutine coordination and keeps a
small amount of worker-result metadata. It is most useful when both the probe
side and the result are large enough to amortize that cost. `B/op` below is
cumulative allocation reported by Go benchmarks, not retained or peak heap.

## Verification

The focused tests cover:

- duplicate numeric, string, and boolean keys;
- NULL and unsupported key values;
- deterministic output across repeated runs;
- one-row inputs, which must fall back without losing a match;
- the `MaxRows` error boundary;
- plan selection via `EXPLAIN ANALYZE`.

The focused race test also passes.

## Benchmark

Five samples used `-benchtime=2s -benchmem` on an AMD Ryzen 9 5950X. Each
iteration joined two 16,384-row sources with four workers for the parallel
path.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Existing sequential hash join | 36,697,536 | 41,918,983 | 196,747 | Reference |
| Opt-in shared-index parallel probe | 31,359,796 | 41,083,763 | 196,803 | 1.17x faster; 1.02x lower bytes; 1.00x allocations |

The parallel path is about 14.5% faster and allocates about 2.0% fewer
cumulative bytes for this workload. It performs about 0.03% more allocations,
so it is not enabled by default.

Raw command:

```text
make m248-ch-g03-benchmark
```

Raw output:

```text
BenchmarkCH003HashJoinBaseline
38184663 ns/op 41919315 B/op 196748 allocs/op
34725632 ns/op 41918974 B/op 196747 allocs/op
36697536 ns/op 41918992 B/op 196747 allocs/op
35729655 ns/op 41918983 B/op 196747 allocs/op
39028050 ns/op 41918909 B/op 196747 allocs/op
BenchmarkCH003ParallelHashJoin
31359796 ns/op 41083935 B/op 196803 allocs/op
32243139 ns/op 41083763 B/op 196803 allocs/op
29895409 ns/op 41083735 B/op 196802 allocs/op
29308561 ns/op 41083689 B/op 196802 allocs/op
33119623 ns/op 41083862 B/op 196803 allocs/op
```
