# M033c Global Timestamp Reservation gRPC Transport

This feature exposes the existing `hatReplication.GlobalTimestampOracle`
reservation contract through an opt-in authenticated gRPC RPC.

## Scope

- `CacheGRPCOptions.GlobalTimestampReserve` installs the coordinator-owned
  handler. A nil handler leaves the RPC unavailable.
- `NewGlobalTimestampReserveGRPCClient` reuses a caller-owned gRPC connection.
- Replication-token authorization is required when the server is configured
  with a replication token.
- Requests are bounded before the handler runs: non-zero term, node identity,
  node epoch, sequence, observed timestamp, and count are required; node IDs
  are limited to 256 bytes and ranges to 1,048,576 timestamps.
- The client verifies term, node identity, epoch, sequence, count, and the
  contiguous positive range before returning a grant.
- Consensus, leader election, TLS, connection lifetime, and durable oracle
  snapshots remain caller-owned.

## Example

```go
oracle, _ := hatReplication.NewGlobalTimestampOracle(7, 0)
server := hatCache.NewCacheGRPCServer(trie, hatCache.CacheGRPCOptions{
    ReplicationAuthToken: "replication-secret",
    GlobalTimestampReserve: func(ctx context.Context, request hatReplication.GlobalTimestampRequest) (hatReplication.GlobalTimestampGrant, error) {
        return oracle.Reserve(request)
    },
})

client := hatCache.NewGlobalTimestampReserveGRPCClient(conn, "replication-secret")
grant, err := client.Reserve(ctx, hatReplication.GlobalTimestampRequest{
    Term: 7, NodeID: "worker-a", NodeEpoch: 3, Sequence: 1, Count: 256,
})
```

Repeated requests with the same oracle identity and sequence remain idempotent
because the underlying oracle owns retry semantics. A response from another
node or with a different range is rejected before it reaches the caller.

## Tradeoff

The transport adds network/RPC work and is intentionally not wired into local
timestamp allocation by default. The coordinator gains a reusable authenticated
wire boundary; callers pay the transport cost only when they need cross-process
or cross-node reservations.

See the raw measurements in [BENCHMARK.md](BENCHMARK.md#m033c-global-timestamp-reservation-grpc-transport).
