# Runtime Memory Report

`GET /api/memory` returns an on-demand snapshot of the Go allocator and
garbage collector. It is modeled after Tarantool slab and memory inspection
surfaces and ClickHouse asynchronous runtime metrics.

The endpoint is read-only, does not trigger garbage collection, does not change
runtime settings, and does not inspect or return cache keys or values. It is
not sampled on normal cache reads or writes. The response is marked
`Cache-Control: no-store`.

## Enable Monitoring

The monitoring HTTP/2 server remains disabled by default:

```text
make monitoring-server MONITORING_ADDR=127.0.0.1:8080
```

For a remote or shared listener, configure an operator token and TLS or put
the listener behind an authenticated reverse proxy:

```text
make monitoring-server MONITORING_ADDR=127.0.0.1:8080 MONITORING_AUTH_TOKEN='replace-with-a-secret'
```

When monitoring authentication is configured, `/api/memory` is covered by the
same authentication middleware as every other `/api/` endpoint:

```text
curl -sS -H 'X-Hatrie-Auth-Token: replace-with-a-secret' http://127.0.0.1:8080/api/memory
```

Without an authentication configuration, the monitoring API is intentionally
unauthenticated, so keep it on loopback or configure authentication before
exposing it.

## Fields

The response contains three groups of values:

- `alloc_bytes`/`heap_alloc_bytes`: live Go heap bytes at the snapshot time.
- `total_alloc_bytes`, `mallocs`, and `frees`: cumulative allocator counters;
  these reveal allocation churn even when live heap is stable.
- `heap_sys_bytes`, `heap_inuse_bytes`, `heap_idle_bytes`, and
  `heap_released_bytes`: reserved, active, idle, and returned heap pages.
- `heap_objects`, stack and runtime metadata fields, and `next_gc_bytes`:
  object count, non-heap runtime costs, and the next GC target.
- `num_gc`, `num_forced_gc`, `pause_total_ns`, `last_gc_unix_nano`, and
  `gc_cpu_fraction`: garbage-collector activity and pause/CPU summaries.
- `go_mem_limit_bytes` and `gogc_percent`: active runtime memory policy.
- `*_class_bytes`: bounded `runtime/metrics` memory classes. They provide a
  more useful breakdown than one heap number; unsupported runtime metrics are
  reported as zero.

For the earlier runaway-allocation symptom, compare `total_alloc_bytes` with
`alloc_bytes` and `heap_objects`. A rapidly rising cumulative counter with a
stable live heap indicates churn. A rising live heap or `heap_inuse_bytes`
indicates retained memory. A large `heap_idle_bytes` with low
`heap_released_bytes` indicates memory reserved by Go but not currently used;
use the authenticated heap profile endpoint for object-level attribution.

Embedded callers can use the same report without HTTP:

```go
report := hatMonitoring.ReadMemoryReport()
```

The root package also exposes `ReadMonitoringMemoryReport` for compatibility
with the rest of the importable cache API.

## Measurement

Run the focused benchmark with:

```text
make benchmark-memory-report
```

Five samples on an AMD Ryzen 9 5950X with Go 1.26.6 produced these medians:

| Path | Time | Heap | Allocs |
| --- | ---: | ---: | ---: |
| `runtime.ReadMemStats` baseline | 14.899 us | 0 B | 0 |
| Full report before sample pooling | 15.789 us | 416 B | 1 |
| Full report after sample pooling | 15.161 us | 0 B | 0 |

The final report is about 1.02x the CPU time of the bare runtime read while
returning the additional allocator, GC, and memory-class data. The one
per-request allocation was removed with a bounded `sync.Pool`; no per-entry
bookkeeping or normal-operation overhead was added.
