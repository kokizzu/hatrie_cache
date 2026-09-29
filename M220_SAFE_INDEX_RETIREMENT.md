# Safe Index Retirement

M220 adds an importable, opt-in lifecycle registry for indexes that may be
replaced or removed while dependent readers are still running.

## Contract

```go
registry := hatSql.NewSQLIndexRetirementRegistry()
if err := registry.Register("accounts_by_region", index); err != nil {
	return err
}

reader, err := registry.Acquire("accounts_by_region")
if err != nil {
	return err
}
defer func() {
	result, released := reader.Release()
	if released && result.Removed {
		closeOrDiscard(result.Index)
	}
}()

use(reader.Index())
```

`Register` rejects blank names, nil or typed-nil indexes, and duplicate names.
`Acquire` admits only active entries and returns a reader lease. `Retire` moves
an entry to `draining`, so new readers are rejected while existing leases keep
the index alive. An idle entry is removed immediately. Otherwise the final
lease release removes it and returns the detached object in
`SQLIndexRetirementResult.Index`.

The registry never closes, frees, or replaces an index. The caller owns that
resource and must dispose of the returned object after `Removed` is true. A
lease release is idempotent: only the first release changes the reader count.
`Status` and `Statuses` expose active/draining state and current reader counts.

This is deliberately not enabled in the SQL executor by default. Existing
query paths retain their behavior and cost. Integrators that publish mutable
index lifetimes can place one lease around the complete dependent operation,
then retire the old index only after the owner has stopped admitting readers.

## Cost

Measured with `make benchmark-m220` on Linux/amd64, AMD Ryzen 9 5950X, Go's
benchmark harness, `-benchmem -count=5 -benchtime=200ms`. Values below are the
median of five samples:

| Operation | ns/op | B/op | allocs/op | Relative to direct pointer |
| --- | ---: | ---: | ---: | ---: |
| Direct pointer access | 0.44 | 0 | 0 | 1.0x |
| Acquire, access, release | 53.0 | 32 | 1 | 120x CPU, 32 B |
| One held lease access | 4.93 | 0 | 0 | 11.2x CPU, 0 B |
| Register then idle retire | 237.3 | 336 | 4 | lifecycle-only |

The direct-pointer comparison is a lower bound, not a complete SQL query
benchmark. The intended usage acquires once per dependent operation and holds
the lease across all index reads; it does not acquire for every row. The
registry is therefore a correctness and ownership tool with a small
per-operation admission cost, not a general query-speed optimization.

Raw benchmark samples are retained in the M220 implementation review history;
rerun `make benchmark-m220` for the local machine and toolchain.
