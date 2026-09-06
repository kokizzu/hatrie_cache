# Deterministic Conflict Resolution

`hat/hatReplication` provides `ConflictVersion` and
`ResolveConflictVersion` for callers that need a deterministic winner when
two writers update the same logical record.

```go
winner, err := hatReplication.ResolveConflictVersion(local, remote)
if err != nil {
    return err
}
if winner == remote {
    apply(remoteValue)
}
```

The ordering is total for valid versions:

1. Larger `Timestamp` wins.
2. For equal timestamps, lexicographically larger `NodeID` wins.
3. For the same node and timestamp, larger `Sequence` wins.

`Timestamp` may be a physical or logical clock value, but all writers must
use the same unit and ordering convention. `NodeID` is required so concurrent
writes cannot depend on arrival order. `Sequence` should be unique for each
distinct write from one node at a given timestamp. Equal versions preserve
the left argument, so callers must not reuse one version for different values.

`CompareConflictVersions` exposes the ordering when a caller needs to make a
decision without selecting a value. Invalid versions return
`ErrConflictVersionInvalid`. The helpers do not mutate state, perform I/O, or
change the existing replication dispatcher.
