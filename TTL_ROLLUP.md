# TTL-Driven Time Rollup

`hatSql.TimeBucketRollup` keeps fixed-width aggregates for timestamped numeric
events and exposes an explicit retention boundary. It is suitable for a
caller-owned TTL job that replaces raw events with verified buckets without a
background goroutine or hidden wall-clock dependency.

```go
rollup, err := hatSql.NewTimeBucketRollup(time.Hour)
if err != nil {
    return err
}
for _, event := range events {
    if err := rollup.Add(event); err != nil {
        return err
    }
}
cutoff := now.UTC().Truncate(time.Hour).Add(-24 * time.Hour)
kept, removed, err := rollup.RetainRawAfterVerified(events, cutoff)
```

Each bucket stores count, sum, minimum, and maximum by key. `Buckets` returns
matching buckets sorted by time and then key. `TimedMetric` timestamps must be
non-zero and values must be finite.

## Safe retention sequence

1. Add all events covered by the intended rollup boundary.
2. Call `VerifyThrough(raw, cutoff)` to compare the stored buckets with a
   deterministic recomputation of raw events before `cutoff`.
3. Persist or otherwise commit the resulting bucket state in the caller's
   durability system.
4. Use `RetainRawAfterVerified` to keep only events at or after the boundary.
5. Call `ExpireBefore(cutoff)` when old buckets themselves should be removed.

Verification and expiration require `cutoff` to be an exact bucket boundary.
Non-boundary requests fail without mutating the rollup. Expiration removes only
buckets whose end is at or before the cutoff, so a partial current bucket is
never discarded accidentally. The API does not perform persistence or claim a
transaction across the raw-event store and rollup; the caller owns that commit
ordering.

Focused coverage is in `hat/hatSql/rollup_ttl_test.go`, including complete and
partial bucket retention, rejected boundaries without mutation, empty and nil
behavior, and an allocation-reporting expiration benchmark.
