# M247 Frontier Expiry Errors

`FrontierRetentionRegistry.Acquire` now returns a typed diagnostic when a
resume/as-of request is older than the frontier's retained lower bound.

## Compatibility

The new error unwraps to the existing `ErrFrontierRetentionExpired` sentinel,
so existing checks continue to work:

```go
lease, err := retention.Acquire("orders", checkpoint.AsOf)
if errors.Is(err, hatPipeline.ErrFrontierRetentionExpired) {
	// The checkpoint is too old for the currently retained history.
}
```

Callers that need recovery details can use `errors.As`:

```go
var expired *hatPipeline.FrontierRetentionExpiredError
if errors.As(err, &expired) {
	fmt.Printf(
		"resume frontier %q expired: requested=%d lower=%d upper=%d\n",
		expired.FrontierID,
		expired.RequestedAsOf,
		expired.CurrentLower,
		expired.CurrentUpper,
	)
}
```

The fields identify the frontier, the requested as-of timestamp, and the
frontier bounds observed when the request was rejected. `RequestedAsOf ==
CurrentLower` is valid; only values strictly below the lower bound are
expired. The upper bound is included for diagnostics and checkpoint logging.

## Recovery

An expired checkpoint cannot be made valid by retrying it. The caller should
obtain a fresh snapshot/checkpoint at or after `CurrentLower`, or surface a
controlled resynchronization request to the client. Do not bypass the error
by replaying history from an older checkpoint: that can duplicate or omit
updates after compaction.

The error is created only on the already-failing expired branch. Successful
frontier checks preserve the prior path and allocation behavior. Calling
`Error()` for structured logging formats the diagnostic string and has the
additional cost recorded in [BENCHMARK.md](BENCHMARK.md#m247-frontier-expiry-errors).
