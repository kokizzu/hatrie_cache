# CH-U25 Query-Wide Spill Quota

`SQLQueryOptions.MaxSpillBytes` remains the existing per-operator limit. The
opt-in `MaxQuerySpillBytes` field adds one live-byte limit shared by every
temporary SQL spill file created during one query.

## Configuration

```go
options := hatSql.SQLQueryOptions{
	MaxSortBytes:      128,
	SpillDirectory:    "/var/lib/hatrie-cache/sql-spill",
	MaxSpillBytes:     64 << 20,
	MaxQuerySpillBytes: 256 << 20,
}
```

The zero value disables the shared ledger. Existing callers therefore retain
their independent `MaxSpillBytes` budgets and execution behavior. A positive
`MaxQuerySpillBytes` still requires the selected operator's existing spill
configuration; it does not turn an in-memory fallback into a spill path.

The quota counts encoded bytes emitted to temporary files, including encryption
or compression framing. Intermediate files release their reservation after
successful deletion, so multi-pass merges are bounded by live temporary data
rather than by the sum of every merge pass. Sort, DISTINCT/set operations,
GROUP BY, and partitioned hash joins all share the same query ledger.

When the limit is reached, execution returns a query spill-budget error and
cleans temporary files created by the query. The ledger is synchronized for
parallel eligible operators. A process crash can leave filesystem orphans
before cleanup runs; operators should use a query-owned spill directory and
apply their normal startup retention/orphan policy.

## Tradeoff

The focused benchmark uses 128 rows, a 128-byte in-memory sort threshold, a
16 MiB per-operator budget, Linux amd64, AMD Ryzen 9 5950X, `-cpu=1`, five
samples per case, and `-benchmem`:

| Mode | Median ns/op | Median B/op | Median allocs/op | CPU vs disabled | B/op vs disabled | Allocs vs disabled |
|---|---:|---:|---:|---:|---:|---:|
| Quota disabled | 13,752,939 | 2,449,695 | 39,367 | 1.00x | 1.00x | 1.00x |
| Quota enabled | 12,983,863 | 2,463,238 | 39,381 | 0.94x | 1.01x | 1.00x |

This is a safety/governance feature, not a speed optimization. On this
serialization-heavy workload, the enabled path measured 0.94x CPU, 1.01x
cumulative allocation bytes, and 1.00x allocations. The CPU difference is
within run-to-run noise and is not a claimed speedup. The quota is off by
default, so callers that do not opt in avoid the ledger's per-write mutex.

### Raw Output

```text
make benchmark-chu25-query-spill-quota
BenchmarkCHU25QuerySpillQuota/quota-disabled          82  13104629 ns/op  2449690 B/op  39367 allocs/op
BenchmarkCHU25QuerySpillQuota/quota-disabled          74  13951483 ns/op  2449695 B/op  39367 allocs/op
BenchmarkCHU25QuerySpillQuota/quota-disabled          93  12768790 ns/op  2449688 B/op  39367 allocs/op
BenchmarkCHU25QuerySpillQuota/quota-disabled          92  13752939 ns/op  2449752 B/op  39367 allocs/op
BenchmarkCHU25QuerySpillQuota/quota-disabled         100  14771256 ns/op  2449698 B/op  39367 allocs/op
BenchmarkCHU25QuerySpillQuota/quota-enabled          100  13559285 ns/op  2463238 B/op  39381 allocs/op
BenchmarkCHU25QuerySpillQuota/quota-enabled           79  12983863 ns/op  2463237 B/op  39381 allocs/op
BenchmarkCHU25QuerySpillQuota/quota-enabled           92  13192046 ns/op  2463239 B/op  39381 allocs/op
BenchmarkCHU25QuerySpillQuota/quota-enabled          100  12218868 ns/op  2463240 B/op  39381 allocs/op
BenchmarkCHU25QuerySpillQuota/quota-enabled           97  12055661 ns/op  2463232 B/op  39381 allocs/op
```
