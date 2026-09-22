# M227: Atomic Snapshot Offset And First Live Frontier

M227 adds an opt-in `hatReplication.ChangefeedSnapshotBoundary` for changefeed
bootstraps. It commits the source snapshot offset and the first live frontier in
one state transition, preventing a restart from persisting only one half of the
bootstrap boundary.

```go
boundary, err := hatReplication.NewChangefeedSnapshotBoundary("orders")
if err != nil {
    return err
}
state, err := boundary.CommitSnapshotBoundary(100, 100)
if err != nil {
    return err
}
_ = state
if _, err := boundary.AdvanceLiveFrontier(101); err != nil {
    return err
}
checkpoint, err := boundary.MarshalBinary()
```

`CommitSnapshotBoundary` accepts `firstLiveFrontier >= snapshotOffset`. An
identical retry is idempotent; a different retry conflicts. Live progress is
rejected until the boundary is committed and cannot regress below either the
committed first frontier or the current live frontier. Restore validates the
same coupling before replacing state.

The CBS1 binary snapshot is bounded and deterministic. It is process-local and
does not itself provide a distributed transaction. The caller must persist the
boundary atomically with the recovered data and source subscription state.

## Benchmark

The benchmark used one committed `orders` boundary on an AMD Ryzen 9 5950X,
Linux amd64, with five samples per case. JSON is a control using the same
exported snapshot struct.

| Operation | CBS1 median | JSON median | CBS1 memory | JSON memory | Improvement |
| --- | ---: | ---: | ---: | ---: | ---: |
| Commit + advance | 17.27 ns/op | not applicable | 0 B/op, 0 allocs | not applicable | 0 allocations |
| Marshal | 72.45 ns/op | 263.0 ns/op | 48 B/op, 1 alloc | 112 B/op, 1 alloc | 3.63x CPU, 2.33x lower B/op |
| Unmarshal | 47.48 ns/op | 1,465 ns/op | 8 B/op, 1 alloc | 272 B/op, 6 allocs | 30.9x CPU, 34.0x lower B/op, 6x fewer allocs |

CBS1 was 38 bytes versus 104 bytes for JSON, or 2.74x smaller. Run the raw
measurements with:

```text
make benchmark-m227-snapshot-boundary
make measure-m227-snapshot-boundary-size
```
