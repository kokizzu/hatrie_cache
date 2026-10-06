# Trusted Stored Procedure Registry

`hatSql.StoredProcedureRegistry` is an opt-in, in-process registry for
versioned command-style handlers. It adopts Tarantool's useful stored-function
idea without adding an embedded scripting runtime or loading executable code
from storage.

## Contract

```go
registry, err := hatSql.NewStoredProcedureRegistry(hatSql.StoredProcedureRegistryOptions{
	Authorize: func(request hatSql.StoredProcedureAuthorization) bool {
		return request.Principal == "worker" && request.RequiredCapability == "orders.execute"
	},
})
if err != nil {
	panic(err)
}

_, err = registry.Register(hatSql.StoredProcedure{
	Name:               "orders.total",
	Version:            "v1",
	RequiredCapability: "orders.execute",
	Execute: func(ctx context.Context, arguments []interface{}) (interface{}, error) {
		return arguments[0].(int64) + arguments[1].(int64), nil
	},
}, "")
if err != nil {
	panic(err)
}

value, err := registry.Invoke(ctx, "worker", "orders.total", []interface{}{int64(2), int64(3)})
```

The first registration uses an empty expected version. A replacement must pass
the currently active version, so stale owners cannot silently replace a newer
generation. `Resolve` and `Snapshot` return metadata only; they never expose
the handler callback.

## Safety Defaults

| Control | Default |
|---|---:|
| Active procedures | 256 |
| Arguments per call | 64 |
| Serialized input | 1 MiB |
| Serialized output | 1 MiB |
| Name/version bytes | 256 |
| Principal bytes | 256 |
| Capability bytes | 256 |
| Authorizer | required at invoke time |

The registry fails closed when no authorizer is configured. A denied request
returns `ErrStoredProcedureAccessDenied`. Handler and authorizer panics are
converted to `ErrStoredProcedurePanic`. Context cancellation is checked before
authorization, before the handler, and after the handler; handlers should also
observe the context while doing work.

Input and output limits use JSON size as a deterministic, transport-neutral
bound. Values that cannot be JSON-marshaled are rejected with
`ErrStoredProcedureValue`. These checks are for trusted in-process callers and
are not a substitute for a sandbox.

## Persistence And Exposure

The registry is not automatically connected to SQL parsing, monitoring, gRPC,
HTTP/2, or replication. Applications must explicitly wire invocation into an
authorized command path. No default server is started.

Only `StoredProcedureMetadata` is suitable for a catalog or snapshot. Handler
callbacks are never serialized, restored, or accepted from untrusted input.
After restart, trusted application code must register the expected versions
again and can compare them with a persisted metadata snapshot.

## Benchmark

The paired benchmark uses `make baseline-tg24-stored-procedure` and
`make benchmark-tg24-stored-procedure` on the same local AMD Ryzen 9 5950X
machine. Five samples are reported; values below are medians.

| Operation | Baseline | Registry | Relative |
|---|---:|---:|---:|
| Versioned lookup | 75.09 ns, 16 B, 1 alloc | 19.37 ns, 0 B, 0 alloc | 3.88x faster, 16 B fewer |
| Lookup plus handler | 84.08 ns, 16 B, 1 alloc | 167.2 ns, 16 B, 2 alloc | 1.99x slower, 1 extra alloc |

The lookup win comes from keeping the active registry keyed directly by the
normalized procedure name. Full invocation intentionally costs more than the
old unprotected function call because it performs authorization, context
checks, argument copying, JSON input/output bounds, and panic isolation. This
feature is retained for its safety and stable lifecycle contract, not as a
claim that protected invocation is faster than an unguarded callback.
