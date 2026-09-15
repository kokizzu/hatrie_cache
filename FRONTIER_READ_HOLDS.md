# Frontier Read Holds

`hatDataStructure.FrontierReadHoldSet` is an opt-in primitive for incremental
readers that need to keep a logical history available while storage compacts.
It follows the Materialize-style `since`/`upper` model for a scalar sequence:

```go
holds := hatDataStructure.NewFrontierReadHoldSet()
hold, err := holds.Acquire(checkpoint, visibleSequence)
if err != nil {
    return err
}
defer hold.Release()

if !hold.Allows(sequence) {
    return errOutsideReadHold
}
safeSince := holds.SafeSince(currentSequence)
if !holds.CanCompactThrough(requestedSequence) {
    return errReaderStillNeedsHistory
}
```

`since` and `upper` are inclusive. `Advance` only moves either bound forward;
`Release` is idempotent so deferred cleanup is safe. `SafeSince` returns the
oldest active `since` bound, or the caller-provided fallback when no hold is
active. Compaction through a sequence equal to an active `since` is rejected.

The registry is deliberately independent of storage and SQL packages so a
caller can wire it to a journal, changefeed, or materialized view without
introducing a write-path allocation. Acquire, advance, and release are O(1);
calculating the minimum is O(active holds). No hold-set map is allocated until
the first hold is acquired.

## Measured Cost

Linux amd64, AMD Ryzen 9 5950X, `go test -bench 'BenchmarkFrontierReadHold'
-benchmem -count=5`:

| Case | Result |
| --- | ---: |
| Empty `SafeSince` | 8.2-9.3 ns/op, 0 B/op, 0 allocs/op |
| One active hold | 36.7-39.4 ns/op, 0 B/op, 0 allocs/op |
| Eight active holds | 54.0-61.4 ns/op, 0 B/op, 0 allocs/op |
| 64 active holds | 490.6-551.8 ns/op, 0 B/op, 0 allocs/op |
| Acquire, advance, release | 82.3-87.7 ns/op, 32 B/op, 1 alloc/op |

The lifecycle allocation is the independent hold handle. Existing callers pay
no cost unless they opt into this registry.
