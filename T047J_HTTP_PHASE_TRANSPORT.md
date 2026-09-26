# T047j HTTP Phase Transport

This feature adds an opt-in HTTP adapter for the existing two-phase cluster
write coordinator. It is intended for deployments where a participant is
reachable through an HTTP control plane or an HTTP-aware proxy. The existing
direct callback and gRPC paths are unchanged, and no HTTP route is registered
automatically.

## Server

Mount the handler on a private replication route owned by the application:

```go
participant, err := hatReplication.NewClusterWriteCommitParticipant(
    hatReplication.ClusterWriteCommitParticipantOptions{MaxRecords: 1024},
)
if err != nil {
    return err
}
mux.Handle("/internal/cluster-write-commit", &hatCache.ClusterWriteCommitHTTPHandler{
    Participant: participant,
    ReplicationToken: os.Getenv("HATRIE_REPLICATION_TOKEN"),
})
```

The handler accepts only `POST` requests with one strict JSON object. Unknown
fields, trailing JSON, invalid phases, invalid digest lengths, and bodies over
64 KiB are rejected. `prepare`, `commit`, and `abort` are idempotent according
to the participant state machine.

## Client

The caller owns the `http.Client`, TLS configuration, endpoint discovery, and
connection lifetime:

```go
client := hatCache.NewClusterWriteCommitHTTPClient(
    endpoint,
    httpClient,
    replicationToken,
)
result, err := hatCache.ExecuteClusterWriteCommitOverHTTP(
    ctx,
    []string{endpoint},
    proposal,
    func(context.Context, string) (*hatCache.ClusterWriteCommitHTTPClient, error) {
        return client, nil
    },
)
```

For a configured token, the handler requires the
`X-Hatrie-Replication-Token` header and compares it in constant time. Use TLS
or a mutually authenticated private network in addition to the token; the
token is an application-level gate, not a replacement for transport security.

## Tradeoff

The HTTP path is a compatibility transport, not a performance fast path. The
measured loopback benchmark performs the full three-phase coordinator with one
participant and compares it with the same coordinator using direct callbacks.
The HTTP server is an in-process `httptest` server, so a real network and TLS
will cost more.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative to direct |
| --- | ---: | ---: | ---: | ---: |
| Direct coordinator | 1,442 | 625 | 13 | 1.00x |
| HTTP phase transport | 159,485 | 28,927 | 254 | 110.60x slower, 46.28x bytes, 19.54x allocs |

Use the existing gRPC phase transport when low latency matters. HTTP remains
useful when interoperability or an existing HTTP control plane is worth the
explicit cost. Because the feature is caller-mounted and opt-in, the default
replication path has no additional request, allocation, or serialization cost.
