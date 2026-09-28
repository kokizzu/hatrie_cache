# T248 Bounded Visibility Queue Dead Letters

`NewVisibilityQueueWithRetryPolicy` is an opt-in wrapper for
`hatDataStructure.VisibilityQueue`. It counts delivery attempts and routes a
lease to a bounded in-memory dead-letter buffer after the configured attempt
limit.

```go
queue, err := hatDataStructure.NewVisibilityQueueWithRetryPolicy[string](
    1024,
    time.Minute,
    1,
    hatDataStructure.VisibilityQueueRetryOptions{
        MaxAttempts:    3,
        MaxDeadLetters: 256,
    },
)
```

The first successful `Lease` has `Attempts == 1`. A terminal `Nack`, or an
expired lease recovered by `RequeueExpired` or the next `Lease`, is moved to
the dead-letter buffer with its stable ID, value, and attempt count. Drain it
with `PopDeadLetter` and use `DeadLetterLen` for backpressure monitoring.

When the dead-letter buffer is full, terminal `Nack` returns `false` and keeps
the active lease. Expiry recovery likewise stops at the full buffer without
discarding the lease. This lets the caller drain or persist failures before
retrying the route.

The legacy constructors, including `NewVisibilityQueue`, remain unchanged.
The retry policy is a separate wrapper, so the default queue retains its
allocation-free hot path and does not retain dead-letter storage. The wrapper
only retains capacity for the configured dead-letter buffer; `PopDeadLetter`
reuses that backing storage after the buffer is drained. The buffer is
process-local and not durable: callers that need restart recovery must persist
dead letters or use a durable command journal.

## Measurement

Run `make benchmark-t248-before-after`. On the measured AMD Ryzen 9 5950X
host with `GOMAXPROCS=1`, seven 2-second samples produced these medians:

| Workload | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| `origin/master` legacy `Lease` + `Nack` | 104.8 | 0 | 0 |
| T248 legacy `Lease` + `Nack` | 101.1 | 0 | 0 |
| T248 primed terminal `Nack` + `PopDeadLetter` | 127.9 | 0 | 0 |

The first two rows overlap normal benchmark noise and show no meaningful
default-path cost. The third row is a different workload that includes the
retry-policy check, bounded dead-letter append, lease removal, and pop; it is
not a claim that terminal handling is faster than ordinary retries.
