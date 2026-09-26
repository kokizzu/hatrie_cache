# T047h gRPC Cluster-Write Phase Transport

This adds an opt-in gRPC transport for the existing transport-neutral
`hatReplication.ExecuteClusterWriteCommit` coordinator.

## Contract

`CacheService.ClusterWriteCommit` carries one validated proposal and one phase:

- `PREPARE` records an idempotent participant reservation.
- `COMMIT` makes a prepared reservation terminal.
- `ABORT` releases a prepared reservation after prepare failure.

The server enables the endpoint only when
`CacheGRPCOptions.ClusterWriteCommitParticipant` is configured. Replication
authentication is checked before any participant mutation. The client sends
the optional `x-hatrie-replication-token` metadata and reuses one caller-owned
gRPC connection for all phases.

`ExecuteClusterWriteCommitOverGRPC` creates one client per participant before
the prepare phase and binds those clients to the existing coordinator. TLS,
dialing, and connection cleanup remain caller-owned. Normal command handling,
asynchronous replication, and default configuration are unchanged.

## Verification

```text
make test-t047-grpc-transport
make race-t047-grpc-transport
make test-t047-grpc-transport-package
make benchmark-t047-grpc-transport
```

The focused test covers prepare, commit, abort, coordinator binding, and
replication-token rejection. The package target also runs the broader existing
packages; unrelated pre-existing `hatSql` failures are reported in the goal
summary when present.

## Tradeoff

On the benchmark host, two direct participant phase calls measured a median
of 79.9 ns/op, 0 B/op, and 0 allocations. Two reused-connection gRPC phase
calls over an in-process `bufconn` measured a median of 61.6 us/op, 24,563
B/op, and 348 allocations, approximately 771x the direct CPU time. This is a
transport cost, not a performance optimization. The feature is retained only
because it supplies a previously missing network contract and has zero cost
when the participant option is left nil.
