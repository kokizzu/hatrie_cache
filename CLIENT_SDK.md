# Client SDK

The language-neutral client contract is defined by
[`proto/hatriecache/v1/cache.proto`](proto/hatriecache/v1/cache.proto). The
contract uses protobuf messages and gRPC methods, including unary health,
statistics, command, snapshot, replication, topology, and election calls plus
command and batch streaming calls.

## Go

The generated contract is available through the public `hat/hatGrpc` package:

```go
import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"hatrie_cache/hat/hatGrpc"
)

ctx := context.Background()
conn, err := grpc.DialContext(
	ctx,
	"127.0.0.1:9090",
	grpc.WithTransportCredentials(insecure.NewCredentials()),
)
if err != nil {
	panic(err)
}
defer conn.Close()

client := hatGrpc.NewCacheServiceClient(conn)
health, err := client.Health(ctx, &hatGrpc.HealthRequest{})
if err != nil {
	panic(err)
}
_ = health
```

`hat/hatGrpc` re-exports the generated service interfaces, stream interfaces,
all protobuf messages, enum types and constants, the service descriptor, and
the protobuf file descriptor. Applications do not need to import the
repository's `internal` generated package.

## Other languages

Install `protoc` and the gRPC plugin for the language and version used by the
client, then generate from the checked-in contract. For example, the command
shape for Python is:

```text
protoc --proto_path=proto \
  --python_out=generated \
  --grpc_python_out=generated \
  proto/hatriecache/v1/cache.proto
```

Use the equivalent `--<language>_out` and gRPC plugin options for Java,
TypeScript, Rust, or another supported language. Keep the generated client and
the `.proto` file from the same compatibility version.

## Compatibility

This package is an import and naming surface only. It aliases the existing
generated protobuf types, so it does not change field numbers, enum values,
RPC names, streaming behavior, serialization, or storage formats. A client
must still use the server's configured address, transport security, and
authentication policy.

For HTTP clients, use the existing HTTP/JSON or NDJSON endpoints documented in
the main README. The protobuf/gRPC API is useful when a strongly typed client,
streaming, or lower protocol overhead is preferred.
