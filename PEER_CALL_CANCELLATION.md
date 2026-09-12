# Peer Call Cancellation

`hatPeer.ConnectionPool.DoWithLifecycleContext` provides an opt-in shared
contract for pooled peer calls. The handler receives a context that is canceled
when either the caller cancels or the connection pool closes. Caller deadlines,
cancellation, and context values are preserved.

```go
err := pool.DoWithLifecycleContext(ctx, func(callContext context.Context, connection hatPeer.Connection) error {
	// Pass callContext to the gRPC, HTTP/2, or compact-protocol operation.
	return runPeerOperation(callContext, connection)
})
```

The existing `ConnectionPool.Do` method is intentionally unchanged. It keeps
its original hot-path cost and remains appropriate when a handler should only
follow the caller context. Use `DoWithLifecycleContext` when an operation must
not outlive the pool or its owning peer session.

## Shutdown Behavior

`Close` marks the pool closed and cancels active lifecycle-aware handlers. A
handler that passes the supplied context to its underlying client can unwind,
release the pooled connection, and let `Close` return. A handler that ignores
the context can still keep `Close` waiting; the `Close` caller's own deadline
continues to bound that wait.

Lifecycle-aware calls use `context.Canceled` when pool shutdown is the source of
cancellation. Caller deadlines still produce `context.DeadlineExceeded`, and
caller cancellation still produces `context.Canceled`.

## Cost And Scope

The lifecycle contract is opt-in because combining a caller context with pool
shutdown has a real cost. For `context.Background()` and `context.TODO()`, the
pool reuses one immutable lifecycle context created at construction, so the
common lifecycle-aware path allocates nothing. A caller context that already
has cancellation or a deadline requires a standard derived context to combine
both cancellation sources.

The API only governs calls made through this pool. It does not automatically
rewrite protocol clients or interrupt arbitrary code. Every gRPC, HTTP/2, or
compact-protocol operation must receive and honor the callback context.

## Benchmark

Five samples were run with `-benchtime=250ms` on Linux/amd64 with an AMD Ryzen
9 5950X. Values below are medians from `make benchmark-t-u47`:

| Path | Median | Memory | Relative to legacy `Do` |
| --- | ---: | ---: | ---: |
| Existing `Do` | 48.73 ns/op | 0 B/op, 0 allocs/op | 1.00x |
| `DoWithLifecycleContext`, background context | 72.28 ns/op | 0 B/op, 0 allocs/op | 1.48x |
| `DoWithLifecycleContext`, cancelable context | 434.5 ns/op | 224 B/op, 4 allocs/op | 8.92x |

The extra work is the price of observing pool shutdown in the same standard
context contract. Keeping the API opt-in avoids changing existing callers;
using a background context avoids per-call heap work. Applications with strict
latency budgets should choose the legacy method unless pool-lifecycle
cancellation is required.

## Verification

The focused tests cover pool-close cancellation, caller cancellation, deadline
and value preservation, connection cleanup, and the unchanged legacy path:

```text
make test-t-u47
make benchmark-t-u47
```

The publisher additionally runs the race detector, `go vet`, and the complete
repository test suite before committing.
