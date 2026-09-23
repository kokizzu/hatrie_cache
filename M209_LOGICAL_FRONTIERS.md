# M209: Monotone Logical Frontiers

M209 adds an opt-in high-watermark guard for historical SQL reads and query
subscriptions. It is useful when a caller combines results from retries,
multiple workers, or multiple source snapshots and must not move its observed
logical time backwards.

The feature is disabled by default. Existing callers that leave the new fields
nil keep the previous stateless read and subscription behavior.

## Read APIs

Create one guard for the consumer or request stream:

```go
frontier := hatSql.NewSQLLogicalFrontier(0)
requested := uint64(42)

result, err := hatSql.ExecuteSQLQueryParameters(ctx, query, resolver, nil,
	hatSql.SQLQueryOptions{
		AsOfFrontier:  &requested,
		LogicalFrontier: frontier,
	})
```

`SQLQueryOptions.LogicalFrontier` applies to `AsOfFrontier` reads. It also
works when `SnapshotToken` is used, because the signed token is normalized to
`AsOfFrontier` before the guard is checked. The sequence is:

1. Verify and normalize the snapshot token, if present.
2. Reject a requested frontier lower than `Current()` before the resolver is
   invoked.
3. Execute the read.
4. Advance the guard only after a materialized result, row stream, or page
   completes successfully.

Thus a failed or cancelled read does not consume a frontier. A rejected read
does not invoke the source resolver and does not mutate the guard.

The guard is safe for concurrent callers. `NewSQLLogicalFrontier` accepts an
initial value, `Current` reads the current value, `Validate` checks a value
without advancing it, and `Advance` atomically accepts only an equal or newer
value. A lower value returns `ErrSQLLogicalFrontierRegression`.

## Subscription APIs

Attach the same guard to `QuerySubscriptionDefinition.LogicalFrontier` when a
consumer needs ordering across initial `AsOf`, `NotifyChangedAt`, and
`Heartbeat` calls:

```go
frontier := hatSql.NewSQLLogicalFrontier(0)
definition := hatSql.QuerySubscriptionDefinition{
	Query:           query,
	AsOf:            42,
	LogicalFrontier: frontier,
}
subscription, err := subscriptions.Subscribe(ctx, definition, resolver, hatSql.QueryOptions{})
```

The exact subscription constructor and definition fields follow the existing
query-subscription API. The guard is checked before a lower initial `AsOf` is
accepted, before a change batch is evaluated, and before a heartbeat changes
subscription state. When several subscriptions share one guard, the guard is
advanced once for the batch.

`nil` remains the compatibility setting. The existing per-subscription
revision/frontier checks still apply independently of this opt-in shared
guard.

## Cost and safety

The guard stores one `uint64` and a mutex; it does not retain rows, snapshots,
or query payloads. The measured normalization path remains allocation-free:
the default path is about 2.29 ns/op and the guarded validation path is about
4.31 ns/op on the benchmark host. The direct equal-value atomic advance is
about 8.38 ns/op, also with zero allocations. These are primitive overheads,
not end-to-end query timings; the feature should be enabled when monotonic
cross-request ordering is needed.

The focused verification commands are:

```text
make m209-test
make m209-race
make m209-vet
make m209-benchmark
```

See the raw samples and comparison table in
[BENCHMARK.md#m209-monotone-logical-frontiers](BENCHMARK.md#m209-monotone-logical-frontiers).
