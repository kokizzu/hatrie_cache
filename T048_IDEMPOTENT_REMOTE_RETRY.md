# T-U48 Idempotent Remote-Call Retry Policy

T-U48 adds hatReplication.RetryRemoteCall, a caller-driven policy for bounded
peer retries. It makes retry safety explicit at the call boundary instead of
assuming that every transport method is safe to repeat.

## Contracts

RemoteCallRead may retry without an identity envelope. RemoteCallIdempotentWrite
requires both a non-empty IdempotencyKey and a non-zero FencingToken. The same
identity is passed to every attempt, with Attempt incremented from one.
RemoteCallNonIdempotent is limited to one attempt; asking for more returns
ErrRemoteCallNonIdempotent before the callback runs.

The callback must put the idempotency key and fencing token into the actual
peer request. The helper does not make a remote endpoint idempotent by itself.
The peer must deduplicate the key and reject stale fencing tokens.

~~~go
value, err := hatReplication.RetryRemoteCall(ctx, func(
	callCtx context.Context,
	request hatReplication.RemoteCallRequest,
) (Response, error) {
	return peer.Apply(callCtx, request.IdempotencyKey, request.FencingToken, payload)
}, hatReplication.RemoteCallPolicy{
	Method:         hatReplication.RemoteCallIdempotentWrite,
	IdempotencyKey: "order-42",
	FencingToken:   leaseToken,
	MaxAttempts:    3,
	InitialBackoff: 10 * time.Millisecond,
	MaxBackoff:     250 * time.Millisecond,
	Jitter: func(_ int, delay time.Duration) time.Duration {
		return delay / 2
	},
	OnRetry: recordRetry,
})
~~~

## Defaults And Observability

Retryable methods default to three attempts, with zero initial backoff. The
maximum attempt count is 64 and maximum backoff is one hour. A zero MaxBackoff
keeps the initial backoff from growing. ShouldRetry defaults to retrying errors
except context cancellation and deadline errors; network-specific callers
should provide a narrower predicate.

Jitter and OnRetry are optional. OnRetry receives method, failed attempt,
next-attempt number, delay, and fencing token, but intentionally omits the
idempotency key so a generic observer does not log it by default.

## Benchmark

The successful one-attempt policy path was measured on an AMD Ryzen 9 5950X
with Go benchmarks, -benchmem, and -count=5:

| Path | Samples (ns/op) | Median | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| Direct local callback control | 0.5391; 0.5244; 0.5151; 0.5374; 0.5078 | 0.5244 | 0 | 0 |
| RetryRemoteCall, one successful attempt | 8.279; 9.066; 8.305; 7.984; 8.372 | 8.305 | 0 | 0 |

The control is intentionally an inlined local call, not a network benchmark.
The policy adds about 7.8 ns of local validation and request-envelope work
with no measured allocation. That is a large relative multiplier against the
artificial control but negligible beside a peer round trip. Failed attempts
also pay the explicitly configured timer/backoff cost, which is the tradeoff
for bounded retry and duplicate-mutation protection.

## Verification

~~~text
make test-t048
make verify-t048
make benchmark-t048
~~~
