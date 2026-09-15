# MZ-002 TypedTable Change Read Holds

`TypedTable` exposes a public Materialize-style read hold for consumers that
read a changefeed over several calls. The hold prevents
`CompactChangesThrough` from discarding history that the reader may still
request. It is opt-in and the hold-set map is allocated only after the first
hold is acquired.

## API

```go
hold, err := table.AcquireChangeReadHold(lastSequence)
if err != nil {
    return err
}
defer hold.Release()

for {
    changes, tail, err := hold.ChangesAfter(lastSequence, 256)
    if err != nil {
        return err
    }
    apply(changes)
    if len(changes) != 0 {
        lastSequence = changes[len(changes)-1].Sequence
    }
    if lastSequence >= tail {
        break
    }
}
```

`AcquireChangeReadHold(since)` snapshots the current table tail as `Upper`.
`ChangesAfter` rejects reads outside that inclusive snapshot, so new writes do
not silently enter a multi-call read. Call `Advance(since)` after a page is
processed to move the lower frontier and extend `Upper` to the current table
tail. `Release` is idempotent and should be deferred immediately.

The hold is intentionally conservative: compaction through a hold's lower
frontier is blocked, and only history strictly below that frontier may be
discarded. This avoids an off-by-one data-loss risk for callers whose sequence
checkpoint and next requested page are updated separately. A caller that has
fully consumed a page should advance to the next sequence it may still need.

Acquiring below the retained `compactedThrough` boundary returns
`ErrTypedTableChangesCompacted`. Acquiring or reading beyond the table's
current snapshot returns `ErrTypedTableChangeReadHoldRange`. Compaction while
an active hold pins the requested sequence returns
`ErrTypedTableChangeReadHoldActive`; after the reader advances or releases,
normal compaction resumes.

## Cost And Verification

The default table path keeps a nil hold-set pointer and does not allocate a
hold map. On an AMD Ryzen 9 5950X Linux amd64 host, the no-hold
`CompactChangesThrough(0)` path measured 8.57 ns/op before the integration and
8.02 ns/op after it, both with zero allocations; this is within benchmark
noise. An explicit acquire/read-hold/release lifecycle measured 100.5 ns/op,
48 B/op, and 2 allocations/op over five samples. Those allocations are paid
only by callers that opt into a multi-call hold.

The clean-overlay verification targets are:

```text
make test-mz002-c226
make test-mz002-all-c226
make race-mz002-c226
make vet-mz002-c226
make benchmark-mz002-before-c226
make benchmark-mz002-c226
```

The table-level hold is a retention guard, not a distributed consensus or
durability protocol. It does not persist hold state across restart, and a
crashed reader must be cleaned up by its owning process or lease layer.
