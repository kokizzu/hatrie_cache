# Replica Read-Only Gate

`hat/hatReplication` provides `ReplicaReadOnlyGate` as a caller-owned guard for
mutation paths that must stop before a replica is promoted, drained, or placed
in maintenance mode.

This is a partial adoption of the replica-wide read-only admission pattern used
by Tarantool-style deployments. It is intentionally importable and does not
change existing `HatTrie` behavior automatically.

## Default

The zero-value options are writable:

```go
gate, err := hatReplication.NewReplicaReadOnlyGate(
	 hatReplication.ReplicaReadOnlyGateOptions{},
)
if err != nil {
	panic(err)
}
```

Read-only mode is therefore off unless the caller enables it. Replication
writes remain allowed while read-only by default; set
`DisableReplicationWrites` when replication must also stop. Operator override
is denied by default; set `AllowOperatorOverride` only for a trusted control
path.

## Protecting A Mutation

Acquire a lease immediately before the mutation and release it on every return
path:

```go
lease, err := gate.Begin(hatReplication.ReplicaMutationOriginLocal)
if err != nil {
	return err
}
defer lease.Release()

return trie.UpsertStringChecked(key, value)
```

The lease holds a read lock. `SetReadOnly` takes the write lock, so it waits for
already admitted leases to finish before returning. Releasing a lease more than
once is safe.

## State Transitions

```go
state, err := gate.SetReadOnly("planned failover")
if err != nil {
	return err
}

// Later, after the node is writable again.
state = gate.SetWritable()
```

`Generation` starts at zero for the default writable state and at one for an
initially read-only gate. Each actual mode or reason transition increments it;
repeating the same read-only state and reason is idempotent. `Reason` is
bounded to 256 UTF-8 bytes and rejects control characters and surrounding
whitespace. A writable state always exposes an empty reason.

`SetWritableWithReason` validates an optional operator event reason and returns
transition errors. The reason is not retained in the writable state; callers
that need an audit trail should record the event in their existing audit log.

## Mutation Origins

| Origin | Writable | Read-only default | Read-only with option |
| --- | --- | --- | --- |
| `ReplicaMutationOriginLocal` | Allowed | Denied | Always denied |
| `ReplicaMutationOriginReplication` | Allowed | Allowed | `DisableReplicationWrites`: denied |
| `ReplicaMutationOriginOperator` | Allowed | Denied | `AllowOperatorOverride`: allowed |

Invalid origins and invalid reasons return sentinel errors suitable for
`errors.Is`. A failed constructor must be treated as a configuration error and
the gate must not be served.

## Integration Boundary

The gate does not discover direct calls such as `DeleteChecked`,
`UpsertCounterChecked`, `PushSliceChecked`, or sketch/filter mutations. Every
caller-owned path that needs protection must acquire a lease. Existing
`MaintenanceReadOnly` and `EnforceLeaderWrites` HTTP/gRPC options remain
unchanged; this gate is an additional importable primitive for code paths that
can be wrapped safely.

It is not an authorization system. The caller must authenticate and authorize
operator and replication paths before passing those origins to `Begin`.

## Verification

Run the focused and full package checks through the Makefile:

```text
make test-chg10-gate
make test-chg10-package
make race-chg10-gate
make vet-chg10-gate
```

The benchmark measures admission overhead rather than a throughput gain. On
the development AMD Ryzen 9 5950X, the direct writable flag control had a
0.2198 ns/op median with zero allocations, while a writable gate lease had a
31.26 ns/op median with one 24 B allocation, or about 142x the control cost.
Use the gate where the safety boundary matters; do not add it to an already
protected inner loop without measuring.
