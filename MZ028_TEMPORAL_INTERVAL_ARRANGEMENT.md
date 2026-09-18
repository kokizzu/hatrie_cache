# MZ-028 Temporal Interval Arrangement

`hatSql.SQLTemporalIntervalArrangement` is an importable, bounded arrangement
for valid-time dimension rows. It groups half-open intervals by a caller-owned
join key and indexes each group by interval start/end, so point-in-time and
range-overlap lookups do not scan every retained interval.

## API

```go
arrangement, err := hatSql.NewSQLTemporalIntervalArrangement(
	hatSql.SQLTemporalIntervalArrangementOptions{MaxIntervals: 100_000},
)
if err != nil {
	return err
}

err = arrangement.Upsert(hatSql.SQLTemporalInterval{
	ID:    "price-2026-01",
	Key:   "sku-42",
	Start: 1_735_689_600_000_000_000, // Unix nanoseconds
	End:   1_738_368_000_000_000_000,
	Row:   hatSql.Row{"price": int64(1299)},
})
if err != nil {
	return err
}

current, err := arrangement.At("sku-42", asOfUnixNanos)
overlapping, err := arrangement.Overlap("sku-42", windowStart, windowEnd)
```

`At` and `Overlap` return detached rows. `Start` is inclusive and `End` is
exclusive. `ID` is globally unique within the arrangement: upserting an
existing ID atomically replaces its previous interval, including when the key
changes. `Delete` removes an ID, and `Snapshot` returns deterministic
key/time/ID order.

The arrangement uses an augmented treap per key. Upsert and delete are
expected O(log n); `At` and `Overlap` are O(log n + k), where `k` is the
number of returned intervals. Queries can return multiple overlapping
versions, which is the distinction from the existing `TemporalTable` point
version helper.

## Bounds and defaults

The zero options value is safe: it allows up to 65,536 intervals and starts no
goroutines. `MaxIntervals` can be set up to 1,000,000. IDs and keys are bounded
to 4,096 bytes. Invalid or zero-width intervals, empty identifiers, and
capacity overflow are rejected without mutating existing state. Rows are
cloned on write and read so callers cannot mutate the arrangement through an
alias.

This is an opt-in data structure. SQL planning does not automatically replace
`TemporalTable` or `IncrementalIntervalJoin`; applications can use it for a
reusable temporal dimension arrangement and feed its detached matches into
their existing join path.

## Measurement and tradeoff

The benchmark uses five 500 ms samples with `-benchmem` on Linux/amd64, AMD
Ryzen 9 5950X.

| Workload | Raw ns/op samples | Median ns/op | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| Existing `TemporalTable` out-of-order point update, 256 versions | 464.1, 464.2, 442.3, 434.3, 434.3 | 442.3 | 411 | 2 |
| Interval arrangement upsert, 256 intervals | 605.5, 636.9, 586.5, 601.1, 620.8 | 605.5 | 538 | 3 |
| Linear interval scan at 4,096 intervals | 6454, 6178, 6225, 5894, 6247 | 6225 | 336 | 2 |
| Interval arrangement `At` at 4,096 intervals | 367.5, 362.7, 346.8, 342.2, 362.0 | 362.0 | 400 | 3 |

For the actual overlapping-interval workload, indexed lookup is about 17.2x
faster, with 19% more temporary bytes and one additional allocation per
detached result. The existing point-version workload is narrower and remains
faster with `TemporalTable`; this feature does not change that default path.
The arrangement retains tree and map metadata by design, so its retained
memory is higher than a scan-only slice. The bounded capacity and opt-in API
keep that cost explicit.

Run the focused checks with:

```text
make test-mz028-temporal-arrangement
make benchmark-mz028-temporal-arrangement
```
