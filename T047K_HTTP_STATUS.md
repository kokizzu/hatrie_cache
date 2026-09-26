# T047k HTTP Participant Status

The T047 two-phase coordinator can persist an `OutcomeUnknown` boundary, and
the participant ledger already exposes a local `Status` method. T047k makes
that status readable through the existing opt-in HTTP phase handler so an
operator or recovery controller can inspect a participant after a process or
network failure.

## Endpoint

Mount the existing handler on a private replication route:

```go
handler := &hatCache.ClusterWriteCommitHTTPHandler{
	Participant:      participant,
	ReplicationToken: os.Getenv("HATRIE_REPLICATION_TOKEN"),
}
mux.Handle("/internal/cluster-write-commit", handler)
```

Use:

```go
client := hatCache.NewClusterWriteCommitHTTPClient(endpoint, httpClient, token)
record, found, err := client.Status(ctx, transactionID)
```

The client sends:

```text
GET /internal/cluster-write-commit?transaction_id=<id>
X-Hatrie-Replication-Token: <token>
```

The response contains `found`, the participant phase (`prepared`,
`committed`, or `aborted`), and the exact proposal fields when a record is
present. A missing record is `found: false`, not an error.

## Safety And Security

- The endpoint is still caller-mounted and is never registered by default.
- Token validation occurs before status lookup and uses the existing constant-
  time comparison.
- The query must contain exactly one bounded `transaction_id`; unknown or
  repeated fields are rejected.
- The operation is read-only. It never commits, aborts, or changes participant
  state, and it does not decide reconciliation policy for the caller.
- Use TLS or a mutually authenticated private network in addition to the
  application token. The token is not a replacement for transport security.

## Measured Cost

`make benchmark-t047k-http-status` on Linux/amd64, AMD Ryzen 9 5950X,
`-benchtime=200ms -count=3`:

| Path | Median ns/op | B/op | allocs/op | Relative latency |
| --- | ---: | ---: | ---: | ---: |
| Direct participant `Status` | 19.1 | 0 | 0 | 1.00x |
| HTTP status transport | 70,085 | 10,822 | 98 | 3,669x slower |

This is a control-plane operation, not a data-plane optimization. The normal
POST phase path remained unchanged in the same three-run benchmark: median
`159,493 ns/op`, `28,970 B/op`, and `254 allocs/op` before the change versus
`158,121 ns/op`, `28,970 B/op`, and `254 allocs/op` after it. Because the
handler is opt-in, the default replication path pays no status-query cost.
