# Read Replica Retries

`hatSql.ReadReplicaSet.ExecuteWithRetry` provides an opt-in retry path for
read-only queries. Each failed attempt advances the existing round-robin
router, so a transient failure can move to another replica without changing
the behavior of callers that continue to use `Execute`.

```go
result, err := replicas.ExecuteWithRetry(
    ctx,
    "FROM CACHE('people') SELECT name WHERE id = ?",
    []interface{}{42},
    hatSql.QueryOptions{},
    hatSql.ReadReplicaRetryOptions{
        MaxAttempts: 3,
        Retryable: func(err error) bool {
            return errors.Is(err, errReplicaUnavailable)
        },
        Backoff: func(failedAttempt int) time.Duration {
            return time.Duration(failedAttempt) * 5 * time.Millisecond
        },
    },
)
```

## Contract

- `MaxAttempts` defaults to one, so retries are off unless explicitly enabled.
- The hard maximum is eight attempts. Values outside `1..8` are rejected.
- A classifier is required when `MaxAttempts` is greater than one. This keeps
  syntax, authorization, and other non-transient errors from being repeated.
- The first attempt uses the next normal round-robin replica; each retry uses
  the next replica as well.
- `Backoff` is optional, receives the failed attempt number starting at one,
  and cannot return a negative duration.
- Backoff waits and zero-delay retry boundaries observe `ctx`; cancellation
  stops the retry sequence and returns the context error.
- The API retries reads only. It does not make writes safe to repeat and does
  not validate that a replica's data is identical to another replica's data.

The caller should classify only errors for which repeating the read is safe.
The attempt cap bounds duplicate work and protects the service from an
accidentally unbounded retry loop.

## Measured Cost

Measured with `go test ./hat/hatSql -run '^$' -bench
'BenchmarkReadReplicaSetExecute' -benchmem -count=5` on Linux/amd64 with an
AMD Ryzen 9 5950X:

| Path | Time | Heap | Allocs |
| --- | ---: | ---: | ---: |
| Existing `Execute`, successful read | 3.36-3.47 us/op | 3,920 B/op | 27/op |
| `ExecuteWithRetry`, one allowed attempt | 3.27-3.53 us/op | 3,920 B/op | 27/op |
| One failure then successful retry | 5.48-5.83 us/op | 5,904 B/op | 38/op |

The single-attempt wrapper is within benchmark noise. A retry adds the cost of
the extra query and result handling, which is intentional resilience overhead,
not a throughput optimization.
