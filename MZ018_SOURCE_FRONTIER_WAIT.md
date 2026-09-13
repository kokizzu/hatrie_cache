# MZ-018 Source Frontier Wait

This adds an opt-in bounded wait for SQL source freshness requirements. It is
inspired by Materialize's use of frontiers to make the visibility point of a
query explicit.

## Usage

```go
result, err := hatSql.ExecuteSQLQueryContext(ctx, query, resolver, hatSql.SQLQueryOptions{
	RequireSourceFrontier:      true,
	RequiredSourceFrontier:     42,
	SourceFrontierWaitTimeout:  2 * time.Second,
	SourceFrontierWaitInterval: 10 * time.Millisecond,
})
```

`SourceFrontierWaitTimeout` is measured from the first frontier check. A zero
or negative timeout preserves the original behavior: perform one check and
reject a source that is unavailable, not ready, or behind. A positive timeout
polls until the source reaches `RequiredSourceFrontier`, the context is
cancelled, or the timeout expires. `SourceFrontierWaitInterval` controls the
poll interval; zero or negative values use the 10 ms default.

The wait is only active when `RequireSourceFrontier` is true. It retries
`ErrSQLSourceFrontierNotReady` and `ErrSQLSourceFrontierBehind`. Resolver
availability errors and other provider errors return immediately because
waiting cannot make those errors correct. A timeout returns
`ErrSQLSourceFrontierWaitTimeout` through `errors.Is` and includes the last
frontier error for diagnosis.

The requirement is applied consistently to materialized execution, row
streaming, offset pagination, and keyset pagination. No goroutine, persistent
state, wire-format change, or storage-format change is added. The default
zero-timeout path retains the existing single validation call.

## Measurement

Command:

```text
make benchmark-mz018-source-frontier
```

The benchmark uses five samples on Linux `amd64`, Go benchmark workers `-32`,
and an AMD Ryzen 9 5950X. The wait-enabled case is already at the required
frontier, so it measures control-path overhead without including a real lag
delay.

| Variant | Median ns/op | B/op | Allocs/op | Relative CPU | Relative bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Immediate check, default | 4,749 | 4,912 | 29 | 1.00x | 1.00x |
| Wait enabled, already fresh | 4,628 | 4,912 | 29 | 0.97x | 1.00x |

The five-sample medians were within normal benchmark noise, with identical
bytes and allocation counts. The latest median was 121 ns/op lower for the
opt-in case, but this is not a claimed speedup. Actual stale-source waits
intentionally add the configured polling delay; that latency is the feature's
availability tradeoff and is not represented by the ready-path comparison.

Raw samples:

```text
BenchmarkMZ018SourceFrontierDefault-32         257816  4749 ns/op  4912 B/op  29 allocs/op
BenchmarkMZ018SourceFrontierDefault-32         223078  4698 ns/op  4912 B/op  29 allocs/op
BenchmarkMZ018SourceFrontierDefault-32         275752  4529 ns/op  4912 B/op  29 allocs/op
BenchmarkMZ018SourceFrontierDefault-32         269367  4798 ns/op  4912 B/op  29 allocs/op
BenchmarkMZ018SourceFrontierDefault-32         237644  5000 ns/op  4912 B/op  29 allocs/op
BenchmarkMZ018SourceFrontierWaitEnabled-32     239570  4628 ns/op  4912 B/op  29 allocs/op
BenchmarkMZ018SourceFrontierWaitEnabled-32     242877  4656 ns/op  4912 B/op  29 allocs/op
BenchmarkMZ018SourceFrontierWaitEnabled-32     248450  4692 ns/op  4912 B/op  29 allocs/op
BenchmarkMZ018SourceFrontierWaitEnabled-32     246478  4557 ns/op  4912 B/op  29 allocs/op
BenchmarkMZ018SourceFrontierWaitEnabled-32     239206  4312 ns/op  4912 B/op  29 allocs/op
```

## Verification

The focused tests cover successful polling, timeout, context cancellation,
the unchanged immediate default, and both offset-style and keyset-style
freshness enforcement. Run:

```text
make test-mz018-source-frontier
make race-mz018-source-frontier
make vet-mz018-source-frontier
```
