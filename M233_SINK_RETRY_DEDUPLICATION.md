# M233 Sink Retry And Deduplication

M233 adds an opt-in bounded retry executor around
`SQLSinkExactlyOnceLedger`. It targets disconnected output connections where a
sink may have applied an idempotency key but the client receives `EOF`, a
timeout, or another transport failure before learning the result.

## Use

```go
executor, err := hatSql.NewSQLSinkRetryExecutor(ledger, hatSql.SQLSinkRetryOptions{})
if err != nil {
	return err
}

result, err := executor.Commit(ctx, commit, func(idempotencyKey string) error {
	return sink.Upsert(ctx, idempotencyKey, rows)
})
if err != nil {
	return err
}
if result.Deduplicated {
	// The ledger already recorded this transaction; no second sink effect ran.
}
```

The callback must pass the supplied idempotency key to the external sink and
the sink must atomically deduplicate that key. M233 cannot make a non-idempotent
external API exactly-once by itself. The existing ledger remains the durable
source of transaction identity and frontier advancement.

## Retry policy

- Default maximum attempts: 3; hard maximum: 64.
- Default initial backoff: 10 ms; default maximum backoff: 1 s.
- Backoff is exponential and context-aware. A canceled context stops before a
  further attempt or during sleep.
- The default classifier retries `io.EOF`, `io.ErrUnexpectedEOF`, closed
  network connections, and timeout or temporary `net.Error` values. Permanent
  application errors are returned immediately.
- `Retryable` can narrow or replace classification for a transport. `Sleep`
  can be injected for deterministic tests.
- A previously committed transaction returns success with `Deduplicated=true`
  and never invokes the external callback.

The executor stores no payloads and has bounded attempts. It is opt-in and
does not change `SQLSinkExactlyOnceLedger` or existing sink commit APIs.

## Measurement

Five 200 ms samples were collected on an AMD Ryzen 9 5950X, `linux/amd64`.
Setup and executor construction were excluded from the timed region.

| Path | Median ns/op | B/op | Allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Existing manual context-aware retry, two attempts | 2,420 | 2,256 | 13 | 1.00x |
| M233 executor retry, two attempts | 2,815 | 2,272 | 14 | 1.16x |
| Existing single successful attempt | 1,917 | 1,960 | 9 | 1.00x |
| M233 executor single successful attempt | 1,896 | 1,960 | 9 | 0.99x |
| Existing already-committed deduplication | 174.8 | 72 | 2 | 1.00x |
| M233 executor already-committed deduplication | 181.0 | 72 | 2 | 1.04x |

The retry path costs 395 ns/op, 16 bytes, and one allocation over the manual
loop, while normal success has no measured allocation or memory increase. The
cost is paid only for callers that opt into automatic retry classification and
bounded backoff; the benefit is deterministic recovery of ambiguous transport
failures without duplicate external effects when the sink honors the key.

Raw samples are in [`M233_BENCHMARK_RAW.txt`](M233_BENCHMARK_RAW.txt), and the
focused checks are run with `make test-m233` and `make race-m233`.
