# TT-015 Read-Worker Tuning

## Scope

Tarantool Vinyl exposes independent worker tuning so remote or disk-heavy
maintenance cannot multiply unbounded read work. Hatrie Cache has no Vinyl
engine or background write worker, but its immutable object-tier cache can run
multiple remote prefetch calls at once. TT-015 is therefore partially adopted
at that boundary.

## Configuration

Set `RemotePartCacheOptions.MaxPrefetchConcurrency` to a positive value when
all prefetch calls sharing one cache must use one remote-read budget:

```go
cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{
	MaxBytes:                64 << 20,
	MaxPrefetchConcurrency: 8,
})
```

The zero value is the default and disables the cache-wide limiter. Each call
still has its own `RemotePartPrefetchOptions.MaxConcurrent` limit. With both
configured, the effective limit is bounded by both values, and the cache-wide
limit is shared across concurrent `Prefetch` calls. Direct `Get` and `Acquire`
calls are not throttled by this option.

Negative values and values above the hard bound are rejected with
`ErrRemotePartCacheInvalidConfig`. A canceled context releases a waiting
worker without starting a loader.

Compaction remains independently controlled by
`CompactionSchedulerOptions.MaxConcurrent` and
`CompactionControllerOptions.MaxPendingBytes`. There is no write-worker option
because this package does not own a Vinyl-style persistent write engine.

## Measurement

The two-caller benchmark showed the following medians:

| Path | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Limit disabled | 22,430 | 5,090 | 70 |
| Limit 2 | 22,962 | 5,189 | 71 |

The enabled path is about 1.02x slower and uses about 1.02x the bytes in this
small CPU-only fixture. That cost is expected for an opt-in overload guard;
the feature is useful when multiple callers would otherwise exceed the remote
service or disk budget, not as a raw latency optimization. Full raw samples
are in [BENCHMARK.md](BENCHMARK.md#tt-015-read-worker-tuning).

## Verification

```text
make test-tt015-read-workers
make race-tt015-read-workers
make benchmark-tt015-read-workers
```

The regression test verifies the shared cap across concurrent callers and
rejects invalid configuration values.
