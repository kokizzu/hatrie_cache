# CH-011 Client Insert Quorum

`InsertQuorum` lets one public cache-command request require a specified
number of successful acknowledgements before the request is reported as
successful. It is the client-facing counterpart to the server-wide
`MonitoringOptions.WriteQuorum` and `CacheGRPCOptions.WriteQuorum` settings.

## Defaults

- `insert_quorum` defaults to `0`, which preserves the existing behavior.
- A positive request value is opt-in and applies only to that request.
- The effective quorum is the larger of the server-wide `WriteQuorum` and the
  request's `InsertQuorum`; a client cannot weaken an operator-configured
  quorum.
- The local successful mutation counts as one acknowledgement. The remaining
  acknowledgements come from successful synchronous replica responses.
- If the quorum is not met, the response is unsuccessful, but the local
  mutation remains applied. This matches the existing server-wide quorum
  semantics and avoids pretending that a remote acknowledgement can roll back
  a completed local write.

The option does not create a queue, retry loop, or durable transaction. A
positive value requires a synchronous replication configuration capable of
serving the requested quorum. The setting is not persisted in cache data.

## HTTP JSON

Send `insert_quorum` on the existing `POST /api/commands` endpoint:

```http
POST /api/commands HTTP/1.1
Content-Type: application/json
Accept: application/json

{"command":"SETSTR","key":"session:42","value":"active","insert_quorum":2}
```

A successful request returns the normal command response:

```json
{"ok":true,"message":"set"}
```

An unsatisfied quorum returns the normal unsuccessful command response and HTTP
`409 Conflict`. The local value is still present and can be read immediately.

## Protobuf and gRPC

The generated `hatriecache.v1.CommandRequest` has the additive field
`insert_quorum = 16`. Set it on unary, streaming, or batch-stream command
requests. The server applies the same validation and effective-quorum rules as
the JSON endpoint.

The Go command contract is `hatCommand.Request.InsertQuorum`. It is shared by
the JSON, protobuf, gRPC, replication, and embedded command paths.

## Validation

The value must be positive when supplied and fit the protobuf `int32` wire
range. It is accepted only on one eligible public mutation. It is rejected on:

- reads and commands that are not already eligible for `WriteQuorum`;
- top-level `BATCH` requests;
- nested batch items; and
- internal replication commands.

Use the existing server-wide `WriteQuorum` when an entire server's public
write policy should be uniform, including batch operations. Use
`InsertQuorum` when a caller needs an explicit durability acknowledgement for
one command while leaving the default server policy unchanged.

## Verification

The focused suite covers JSON and protobuf round trips, direct command
execution, global-quorum precedence, HTTP handling, gRPC request conversion,
invalid values, and batch rejection. Run:

```sh
make test-ch11
make verify-ch11
```

The benchmark and its raw samples are in
[BENCHMARK.md](BENCHMARK.md#ch-011-client-insert-quorum).
