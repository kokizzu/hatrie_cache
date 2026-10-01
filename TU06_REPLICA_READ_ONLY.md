# T-U06: Replica-Wide Read-Only Admission

T-U06 adopts the useful part of Tarantool's replica `read_only` behavior: a
trie can fence public mutations while still accepting trusted internal
replication traffic needed to catch up.

## Behavior

The zero value remains writable. Enable the gate on an embedded trie:

```go
trie.SetMaintenanceReadOnly(true)
defer trie.SetMaintenanceReadOnly(false)
```

`MaintenanceReadOnly()` reports the current state. Public `ExecuteCommand`
writes, SQL mutations, RowBinary import batches, and SQL transaction commits
return `node is in maintenance read-only mode`. Reads remain available.

`INTERNALSET`, `INTERNALDEL`, and the existing internal replication envelopes
are exempt so a replica can apply authenticated replication traffic. Existing
HTTP and gRPC replication authorization remains responsible for deciding who
may submit those commands.

Passing `MaintenanceReadOnly: true` to `NewMonitoringHandler` or
`NewCacheGRPCServer` also enables the shared trie gate. Passing `false` does
not turn off a gate enabled by another owner; call
`SetMaintenanceReadOnly(false)` for an explicit operator override.

This is a public admission boundary, not a lock injected into every typed
mutation method. Callers that use low-level methods such as
`UpsertStringChecked` directly must honor `MaintenanceReadOnly()` or route
the mutation through `ExecuteCommand`/SQL. This keeps the default path small
and avoids silently changing trusted recovery code.

## Verification

```sh
make test-tu06-replica-read-only
make race-tu06-replica-read-only
make vet-tu06-replica-read-only
```

The tests cover the default-off behavior, public writes, atomic batches, SQL
mutations, staged SQL transaction commits, reads, internal replication
deletes, explicit disablement, and HTTP/gRPC constructor wiring.

## Measurement

The matched workload repeatedly executes `SET` through `HatTrie.ExecuteCommand`
with the gate disabled. Linux/amd64, AMD Ryzen 9 5950X, `-benchmem`.

| Path | ns/op | B/op | allocs/op | Relative CPU |
|---|---:|---:|---:|---:|
| Before gate, pre-change sample | 210.7 | 0 | 0 | 1.00x |
| After gate, five-run median | 213.5 | 0 | 0 | 1.01x (1.3% slower) |

After raw samples were `213.5`, `212.7`, `220.0`, `230.6`, and `209.7`
ns/op. The single pre-change sample is not enough to claim a speedup; the
observed difference is normal run variance. The default-off cost is one
atomic load and a branch, with no measured allocation or heap increase.
