# Rollup TTL Pruning

`TimeBucketRollup.ExpireBefore` provides an explicit retention operation for
completed time buckets. Callers can pass `now.Add(-retention)` at their chosen
maintenance cadence. The cutoff must align to the rollup width; buckets ending
after the cutoff are retained, including the boundary bucket.

```go
cutoff := time.Now().UTC().Add(-7 * 24 * time.Hour)
removed, err := rollup.ExpireBefore(cutoff.Truncate(time.Hour))
```

The method is synchronous and does not start a background goroutine. This keeps
retention scheduling, persistence, and failure handling under the caller's
control. It only changes the in-memory rollup; raw-event deletion should still
use `VerifyThrough` and `RetainRawAfterVerified` when raw data is being removed.

## Measurement

Run:

```text
make benchmark-rollup-ttl
```

The focused development-host benchmark pruned 512 of 1,024 in-memory buckets:

| Workload | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| 1,024 buckets, 512 expired | 39,151 | 18 | 0 |

Rerun it for current-host timing; exact nanosecond values vary with compiler
and scheduler versions.
