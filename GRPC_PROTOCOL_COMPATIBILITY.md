# gRPC Protocol Compatibility

`hatCache.CacheGRPCOptions.ProtocolVersions` adds an opt-in compatibility
range for rolling binary upgrades. The zero value accepts the current protocol
version (`1`), preserving existing clients and server construction.

Configure a server's overlap window during a rollout:

```go
server := hatCache.NewCacheGRPCServer(trie, hatCache.CacheGRPCOptions{
	ProtocolVersions: hatCommand.ProtocolVersionRange{Min: 1, Max: 2},
})
hatCache.RegisterCacheGRPCServer(grpcServer, server)
```

Clients can advertise the versions they understand through the public
`hatGrpc` package:

```go
ctx := hatGrpc.AppendProtocolVersionRange(
	context.Background(),
	hatCommand.ProtocolVersionRange{Min: 1, Max: 2},
)
health, err := client.Health(ctx, &hatGrpc.HealthRequest{})
```

The server selects the highest overlapping version and returns it in
`x-hatrie-protocol-version`, with the complete server range in
`x-hatrie-protocol-supported`. Missing metadata is treated as an exact request
for the current version. Malformed metadata returns `InvalidArgument`; a
valid range with no overlap returns `FailedPrecondition`. The check runs once
per unary RPC or stream, before command, batch, snapshot, topology, election,
and replication work. Authentication still runs first.

A safe rollout keeps an overlap until all clients have moved:

1. Deploy servers accepting the old and new ranges.
2. Upgrade clients and have them advertise the new range.
3. Confirm compatibility failures are absent.
4. Remove the old version from the server range only after old clients are gone.

This is a wire-compatibility gate, not schema migration or consensus. It does
not make protobuf fields compatible automatically, and it does not coordinate
multiple nodes. Rolling schema changes remain a separate concern.

## Cost

The legacy omitted-metadata path is allocation-free. Explicit negotiation
parses metadata once per RPC or stream; it is not repeated for each streamed
message. See [BENCHMARK.md](BENCHMARK.md#grpc-protocol-compatibility-gate)
for raw samples.

## Verification

```sh
make test-grpc-protocol-gates-local-clean
make test-grpc-protocol-gates-race-local-clean
make vet-grpc-protocol-gates-local-clean
make benchmark-grpc-protocol-gates-local-clean
```
