# Versioned Configuration Watch

`hatTopology.ConfigWatchLog` is a bounded, authenticated change log for
configuration updates. It provides replay by version, explicit history-gap
errors, and a context-aware `Wait` operation for reconnecting stream adapters.

This is the storage and cursor contract for a cluster-wide watch. It does not
open a listener, discover peers, implement consensus, or serialize network
frames. The caller supplies the globally ordered version and maps `Read` or
`Wait` to its authenticated gRPC, HTTP/2, or other transport.

## Example

```go
log, err := hatTopology.NewConfigWatchLog(hatTopology.ConfigWatchOptions{
	HistoryLimit:  256,
	MaxKeyBytes:   256,
	MaxValueBytes: 64 << 10,
	Authorizer: func(ctx context.Context, request hatTopology.ConfigWatchAuthorization) error {
		return authorizeConfig(ctx, request.Principal, request.Action, request.Key)
	},
})
if err != nil {
	return err
}

if err := log.Publish(ctx, principal, hatTopology.ConfigWatchEvent{
	Version: consensusVersion,
	Source:  nodeID,
	Key:     "limits/request_bytes",
	Value:   []byte("1048576"),
}); err != nil {
	return err
}

events, nextVersion, err := log.Wait(ctx, hatTopology.ConfigWatchRequest{
	Principal:    principal,
	AfterVersion: lastVersion,
	Limit:        64,
})
if err != nil {
	var gap *hatTopology.ConfigWatchGapError
	if errors.As(err, &gap) {
		// Fetch a fresh snapshot, then resume at gap.EarliestVersion-1.
	}
	return err
}
_ = events
_ = nextVersion
```

The authorizer is mandatory. Use separate logs or an equivalent scoped
authorization boundary when different principals must not see the same
configuration keys: a `Read` request consumes the log's ordered stream, so it
does not provide per-key filtering.

## Semantics

- `HistoryLimit` defaults to 256 events and is capped at 65,536.
- `MaxKeyBytes` defaults to 256 and is capped at 4,096.
- `MaxValueBytes` defaults to 64 KiB and is capped at 16 MiB. The configured
  history/value product is capped at 64 MiB of worst-case retained values.
- Every event requires a non-empty bounded `Source` and `Key`. Values are
  copied on publish and copied again on read, so callers cannot mutate retained
  history through a reused buffer.
- Version zero is assigned as the next local version. A distributed publisher
  must provide a globally ordered version from its consensus or fencing layer;
  stale or duplicate versions are rejected.
- `Read` returns events after `AfterVersion` up to `Limit`. Its cursor is the
  last delivered version, which makes small batches safe to resume.
- `Wait` returns immediately when events are available, otherwise sleeps on one
  shared notification channel until a publish or context cancellation. Idle
  clients do not create one goroutine or unbounded queue each.
- When ring eviction makes a cursor unrecoverable, the result is a
  `*ConfigWatchGapError` matching `ErrConfigWatchHistoryGap`. Obtain a fresh
  snapshot and resume from the retained boundary.
- Delete events set `Deleted: true` and must not contain a value. The log does
  not reconstruct current configuration state; snapshot management belongs to
  the caller.

The `Source` field identifies the node that produced an event. It is metadata,
not authentication. The transport and caller must authenticate the principal,
authorize the operation, enforce tenant isolation, and reject stale topology or
fencing generations. Do not publish raw credentials, tokens, or private keys
as values; retained history and any transport adapter can expose them to every
authorized reader of that log.

## Benchmark

Measured locally on Linux/amd64 with an AMD Ryzen 9 5950X. Each row is the
median of five `go test -benchmem` runs. The publish/read workload stores one
small value and reads one event. The publish/wait workload publishes an event
and immediately waits from the previous cursor.

| Workload | Median ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| Publish plus one-event `Read` | 331.5 | 216 | 5 |
| Publish plus immediate `Wait` | 284.5 | 192 | 2 |

These measurements include authorization, version/range checks, ring insertion,
value copying, and read-result copying. Retention is bounded independently of
the number of idle clients because clients poll or wait against the same log
notification channel.

Raw benchmark and verification commands:

```text
make benchmark-t-u50
make test-t-u50
make verify-t-u50
```
