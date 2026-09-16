# Explicit Async Insert Acknowledgment Modes

The monitoring command API now supports an explicit ClickHouse-style wait mode
for its opt-in asynchronous journal admission path. It makes the completion
boundary visible without changing the default response or enabling async
commands by accident.

The feature is available only when `MonitoringOptions.AsyncCommands` is enabled,
the request uses `X-Hatrie-Async: true` or `Prefer: respond-async`, and the
request has a valid idempotency key.

## Modes

Use the `wait_for_async_insert` query parameter on `POST /api/commands`:

| Value | Behavior |
|---:|---|
| omitted or `0` | Admit the journaled write and return `202` while it is pending. Poll `/api/commands/status` with the idempotency key. |
| `1` | Admit the write, wait for journal durability and in-memory application, and return the completed response. |

The parameter is deliberately numeric and accepts only `0` or `1`. It has no
effect on ordinary synchronous requests. An invalid value returns `400` before
the command is admitted.

```sh
curl --fail-with-body \
  -H 'Authorization: Bearer operator-secret' \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -H 'X-Hatrie-Async: true' \
  --data '{"command":"SET","key":"session:42","value":"ready","idempotency_key":"session-42-write-1"}' \
  'http://127.0.0.1:8080/api/commands?wait_for_async_insert=1'
```

A successful waited request returns `200` with the normal accepted response
shape and `status: "completed"`. A command-level rejection returns `409` with
the completed command response. If the HTTP request deadline expires, the
server returns `408`; the already-admitted command continues and can still be
retrieved from the status endpoint using its idempotency key.

## Cost And Boundary

The wait mode is a correctness and observability control, not a throughput
optimization. The wait is against `CommandJournalSubmission.Wait`, whose
completion point is after journal sync and in-memory application. It does not
cancel an admitted write when the caller times out.

The benchmark used `-benchtime=100ms -count=3` on an AMD Ryzen 9 5950X with
`-benchmem`:

| Path | Median time | Bytes/op | Allocs/op | Relative time |
|---|---:|---:|---:|---:|
| Parent admission path | 35.130 us | 10,435 | 58 | 1.00x |
| Current admission path | 33.734 us | 10,437 | 58 | 0.96x time |
| Current `wait_for_async_insert=1` | 0.710 ms | 11,751 | 63 | 20.2x vs parent admission |

The admission-path difference is within benchmark noise and has identical
allocation count. The waited path intentionally pays for durable completion:
about `+0.677 ms`, `+1,314 B/op`, and `+5 allocs/op` in this run. Keep it for
callers that need an acknowledgment; use mode `0` for maximum admission
throughput.

Raw samples are recorded in [BENCHMARK.md](BENCHMARK.md#chu03-explicit-async-insert-acknowledgment-modes).

## Security And Compatibility

The feature does not weaken monitoring authentication, RBAC, idempotency
validation, write-only checks, or replication compatibility guards. The
existing default-off behavior remains unchanged. Keep idempotency keys opaque
and avoid putting sensitive business data in them because status URLs include
the key.
