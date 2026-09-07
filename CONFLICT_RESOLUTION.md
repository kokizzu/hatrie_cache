# Deterministic Conflict Resolution

`hatReplication.ConflictVersion` identifies a write with a caller-supplied
timestamp, writer node ID, and per-writer sequence. `CompareConflictVersions`
orders valid versions by timestamp, then lexical node ID, then sequence.
`ResolveConflictVersion` returns the larger version; equal versions preserve
the left value.

The ordering is deterministic for concurrent writers and independent of
arrival order. Missing node IDs are rejected. This is a pure conflict-decision
primitive: it does not perform replication, quorum acknowledgement, or
metadata consensus, so callers must apply the selected version atomically in
their own storage path.
