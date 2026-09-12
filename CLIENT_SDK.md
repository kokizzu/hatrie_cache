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

## HTTP command helper

For callers that prefer language-neutral HTTP/JSON, the importable
`hat/hatMonitoring` package provides a typed helper for `POST /api/commands`:

```go
import (
	"context"

	"hatrie_cache/hat/hatCommand"
	"hatrie_cache/hat/hatMonitoring"
)

client := hatMonitoring.NewClient("http://127.0.0.1:8080", "read-write-token")
response, err := client.Command(ctx, hatCommand.Request{
	Command: "SETSTR",
	Key:     "name",
	Value:   "ivi",
})
```

For several commands, `Client.Batch` reuses the server's `BATCH` command and
sends them in one HTTP request. Pass `true` for the existing atomic batch mode
when all commands must succeed together.

```go
response, err := client.Batch(ctx, []hatCommand.Request{
	{Command: "SETSTR", Key: "one", Value: "1"},
	{Command: "SETSTR", Key: "two", Value: "2"},
}, false)
```

`Command` and `Batch` use JSON by default. For clients where wire bandwidth is
more important than single-command CPU, select the protobuf command contract
once on the client:

```go
client.CommandWireFormat = hatCommand.CommandWireFormatProtobuf
response, err := client.Command(ctx, hatCommand.Request{
	Command: "SETSTR",
	Key:     "name",
	Value:   "ivi",
})
```

The equivalent per-call methods are `CommandWithFormat` and `BatchWithFormat`.
For an atomic string compare-and-swap, send `hatCommand.Request{Command: "CAS",
Key: "name", ExpectedValue: "old", Value: "new"}`. The response uses
`Value == "1"` for a swap and `Value == "0"` when the key is absent, not a
string, or has changed. CAS keeps an existing expiration.
They send `Content-Type` and `Accept: application/x-protobuf` and require the
same protobuf response content type. JSON remains the default because the
repository benchmark shows protobuf is a bandwidth win and a batched latency
win, but is slower for one small command or ten separate requests. See
`BENCHMARK.md` for the measured CPU, heap, allocation, and wire-size results.

The equivalent wire request is usable from any language with an HTTP client:

```sh
curl -sS \
  -H 'Authorization: Bearer read-write-token' \
  -H 'Content-Type: application/json' \
  -d '{"command":"SETSTR","key":"name","value":"ivi"}' \
  http://127.0.0.1:8080/api/commands
```

`Client.Health` and `Client.Entries` remain available for monitoring reads.

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
generated protobuf types, so it does not change enum values, RPC names,
streaming behavior, or storage formats. The HTTP command helper additionally
supports the opt-in protobuf wire format; its `atomic` batch flag is field 14
and remains backward-compatible with older protobuf readers. A client must
still use the server's configured address, transport security, and
authentication policy.

For HTTP clients, use the existing HTTP/JSON or NDJSON endpoints documented in
the main README. The protobuf/gRPC API is useful when a strongly typed client,
streaming, or lower protocol overhead is preferred.
## Command Request Compression

Command and `BATCH` requests are uncompressed by default. To reduce bandwidth
for larger JSON or protobuf requests, set a positive byte threshold on the
client:

```go
client := hatMonitoring.NewClient("http://127.0.0.1:8080", token)
client.CommandCompressionThreshold = 64 << 10
```

Requests whose encoded body reaches the threshold use `Content-Encoding: gzip`.
The setting applies to both `Command`/`Batch` and their `WithFormat` variants;
the threshold is evaluated after JSON or protobuf encoding. Keep it at `0`
(the default) for small commands or CPU-sensitive links because gzip adds
compression work and allocations, and can make tiny bodies larger.
