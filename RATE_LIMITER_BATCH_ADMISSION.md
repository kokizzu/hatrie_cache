# Atomic Batch Rate-Limiter Admission

`hatRate.RateLimiter.AllowN` reserves multiple tokens for one client while
holding the client's shard lock once. The request is all-or-nothing: a failed
batch does not consume a partial number of tokens. `Allow` keeps its dedicated
single-request path, so existing callers do not pay the generalized batch
branch.

```go
if !limiter.AllowN("tenant-a", 32) {
	return ErrRateLimited
}
```

This incorporates a common systems pattern from the compared products:
ClickHouse-style workload admission, Materialize-style bounded operator
backpressure, and Tarantool-style batched requests all amortize coordination
over a logical batch. The limiter remains sharded and lazily initializes client
maps; this change does not increase its retained state.

## Semantics

- `count <= 0` is accepted without changing state.
- A request larger than the configured burst limit is rejected without
  changing state.
- Refill is calculated once per batch using the existing token-bucket window.
- `Allow` retains the previous behavior and fast path.

## Measurement

Machine: AMD Ryzen 9 5950X, linux/amd64, Go benchmark with
`-benchmem -count=5`.

Commands:

```text
make benchmark-rate-limiter-allow-n-baseline
make benchmark-rate-limiter-allow-n
make benchmark-rate-limiter-single-baseline
make benchmark-rate-limiter-single
```

| Workload | Samples (ns/op) | Median | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| Before: eight repeated `Allow` calls | 297.2, 313.2, 288.6, 271.3, 278.0 | 288.6 | 0 | 0 |
| After: one `AllowN(..., 8)` call | 35.91, 39.43, 35.53, 33.73, 35.08 | 35.53 | 0 | 0 |
| Before: single `Allow` | 36.94, 35.63, 34.73, 33.39, 34.10 | 34.73 | 0 | 0 |
| After: single `Allow` | 35.04, 36.09, 34.86, 34.96, 34.28 | 34.96 | 0 | 0 |

Batch admission is about `8.12x` faster for eight requests. Single-request
performance is effectively unchanged within run-to-run noise (`0.66%` higher
median in this sample), and memory/allocation behavior is unchanged.

Verification:

```text
make test-rate-limiter-allow-n
make test-rate-limiter-package
make race-rate-limiter-allow-n
```
