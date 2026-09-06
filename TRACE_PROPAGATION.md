# Trace Propagation

Hatrie Cache propagates an existing W3C Trace Context `traceparent` through
remote replication. This is transport metadata only: the server does not
create trace IDs automatically, and requests without a valid trace context
keep the existing wire shape.

## HTTP

The monitoring handler extracts a valid `traceparent` request header. HTTP
replication copies that context to the outgoing `traceparent` header, including
when the request is retried. Invalid headers are ignored. Authentication and
content-encoding headers are independent and are still handled as before.

## gRPC

The gRPC server extracts `traceparent` from incoming metadata. gRPC replication
adds the same value to outgoing metadata while preserving authorization and
replication-token metadata. The gRPC metadata key is lowercase, as required by
gRPC metadata rules.

## Wire format and validation

Only W3C version `00` is accepted:

```text
00-<32 hex trace id>-<16 hex span id>-<2 hex trace flags>
```

Trace and span IDs must be non-zero. Malformed or zero IDs are ignored rather
than returned as errors from a remote request. The public transport-neutral
helpers live in `hatrie_cache/hat/hatTrace`:

- `ParseTraceParent` and `NewSpanContext` validate values.
- `WithTraceParent` and `ExtractHTTPContext` install validated context.
- `TraceParentFromContext` and `InjectHTTP` emit validated context.
- `HTTPMiddleware` extracts context before the wrapped handler runs.

This propagation does not authenticate trace metadata or change authorization.
Operators should continue to restrict monitoring and replication endpoints and
should treat trace IDs as untrusted correlation data.

## Verification

Focused tests cover round trips, malformed values, HTTP outgoing replication,
gRPC outgoing metadata, and inbound gRPC extraction. Run:

```text
make test-c135-transport-clean
```
