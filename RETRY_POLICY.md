# Method-Aware Remote Retry Policy

`hatPeer.RetryPolicy` is an opt-in bounded retry executor for gRPC, HTTP/2,
compact-protocol, or other remote calls. It distinguishes safe reads,
idempotent mutations, and non-idempotent mutations before deciding whether a
failed call can run again.

```go
policy := hatPeer.NewDefaultRetryPolicy()
err := policy.Execute(ctx, hatPeer.RetryRequest{
	Operation:      hatPeer.RetryIdempotentMutation,
	IdempotencyKey: "order-42-update-7",
	FencingToken:   17,
}, func(callContext context.Context, attempt hatPeer.RetryAttempt) error {
	// Send the same idempotency key and fencing token on every attempt.
	return sendPeerMutation(callContext, attempt)
})
```

## Safety Rules

- `RetryRead` may retry when `Retryable(error)` classifies the error as
  transient.
- `RetryIdempotentMutation` requires a non-empty idempotency key and a
  non-zero fencing token before the first attempt is made.
- `RetryNonIdempotentMutation` is always limited to one attempt, even when the
  policy allows more attempts.
- Every retry receives the same idempotency key and fencing token, with a
  monotonically increasing attempt number.
- Cancellation and deadlines stop execution before another attempt or during
  backoff; context errors are never retried.

The policy does not create a remote deduplication database. The receiving
service must atomically record the idempotency key, reject stale fencing tokens,
and make the mutation result replayable if duplicate suppression is required.
The fencing token should come from the caller's current leadership or lease
generation, not from a retry counter.

## Defaults

`NewDefaultRetryPolicy` uses three total attempts, a 10 ms initial backoff, a
250 ms maximum backoff, and symmetric 20 percent jitter. The policy caps
configured attempts at eight and idempotency keys at 256 bytes. Set
`DisableJitter` for deterministic schedules, inject `Sleep` for a custom
scheduler or tests, and use `Observer` for retry metrics or logs.

The default error classifier retries non-context errors. Production callers
should normally supply a classifier that recognizes only transport failures
such as unavailable or reset connections; application validation errors should
not be retried.

## Backoff And Cancellation

Backoff doubles after each failed retry until `MaxBackoff`, then applies the
configured jitter and clamps to the maximum. The injected sleep function
receives the execution context, so a caller deadline or cancellation can
interrupt a long delay immediately.

Observer events are emitted before each delay and include the request identity,
failed attempt, next attempt, delay, and error. Observers should avoid blocking
the call path.

## Measurement

Five samples with `-benchtime=250ms` on Linux/amd64 and an AMD Ryzen 9 5950X
gave these medians from `make benchmark-t-u48`:

| Path | Median | Memory |
| --- | ---: | ---: |
| Direct callback baseline | 0.49 ns/op | 0 B/op, 0 allocs/op |
| One successful policy attempt | 6.11 ns/op | 0 B/op, 0 allocs/op |
| Three attempts, injected zero-delay sleep | 24.18 ns/op | 0 B/op, 0 allocs/op |

The policy adds about 5.62 ns to a successful call in this microbenchmark and
does no work for callers that do not opt in. Real network latency and timer
delays dominate these local callback costs.

## Verification

Focused tests cover stable mutation identity, non-idempotent single-attempt
behavior, identity validation, cancellation during backoff, final-error
preservation, capped exponential delays, and idempotency-key limits:

```text
make test-t-u48
make benchmark-t-u48
```

The publisher additionally runs the race detector, `go vet`, and the complete
repository test suite.
