# C153 Partition Ownership Consensus over gRPC

This is the transport portion of the ClickHouse/Materialize/Tarantool-inspired
partition metadata consensus work. The existing `hatTopology` package owns
the deterministic quorum decision, HMAC vote authentication, bounded wire
codec, and cancellation-aware collector. This feature adds an opt-in gRPC
vote endpoint and a client adapter for that collector.

## Behavior

`CacheGRPCOptions.PartitionOwnershipConsensusVote` enables the endpoint. The
server sends the proposed shard, primary, ordered replicas, topology
fingerprint, and fencing token to a caller-owned handler. The handler returns
one local `PartitionOwnershipConsensusVote`.

The server:

- requires the existing replication token when `ReplicationAuthToken` is configured;
- rejects malformed ownership metadata before invoking the handler;
- binds the response identity to `CacheGRPCOptions.NodeName`;
- optionally verifies the returned HMAC vote with
  `PartitionOwnershipConsensusAuthenticator`;
- returns `Unavailable` when the handler is not configured.

`NewPartitionOwnershipConsensusGRPCClient` and
`CollectPartitionOwnershipConsensusOverGRPC` adapt the endpoint to the
transport-neutral collector. The caller owns TLS, dialing, connection reuse,
timeouts, and connection cleanup.

## Defaults and limits

The handler is nil by default, so no new RPC work occurs on normal cache or
replication paths. A request accepts at most 4,096 replicas and 1 MiB per
ownership string. HMAC authentication is optional at the API layer, but
replication-token authentication remains available through the existing gRPC
configuration.

Example setup:

```go
authenticator, _ := hatTopology.NewPartitionOwnershipConsensusAuthenticator(
    "cluster-key-v1", []byte("0123456789abcdef0123456789abcdef"),
)
server := hatCache.NewCacheGRPCServer(nil, hatCache.CacheGRPCOptions{
    NodeName:             "node-b",
    ReplicationAuthToken: "replication-secret",
    PartitionOwnershipConsensusAuthenticator: authenticator,
    PartitionOwnershipConsensusVote: func(ctx context.Context, proposal hatTopology.PartitionOwnership) (hatTopology.PartitionOwnershipConsensusVote, error) {
        return authenticator.Sign(hatTopology.PartitionOwnershipConsensusVote{
            NodeID: "node-b", Ownership: proposal, Accepted: true,
        })
    },
})
```

This is a vote transport, not an automatic consensus coordinator: topology
publication, durable control-plane state, TLS policy, and connection lifecycle
remain explicit caller responsibilities.

## Cost

Measured on Linux/amd64 with `make bench-c153g-ownership-grpc`, three 200 ms
runs, bufconn, HMAC verification, and `-benchmem`:

| Path | Median ns/op | Median B/op | Median allocs/op | Relative latency |
| --- | ---: | ---: | ---: | ---: |
| Direct bounded collector callback | 8,259 | 3,081 | 41 | 1.00x |
| gRPC vote fetch | 50,023 | 17,878 | 255 | 6.06x |

The gRPC path is intentionally slower and allocates more because it provides
remote transport, protobuf framing, metadata authentication, and server-side
validation. It is not a regression to local operation: it is opt-in and the
existing local collector remains unchanged.

Verification:

```text
make test-c153g-all
make race-c153g-ownership-grpc
make bench-c153g-ownership-grpc
```
