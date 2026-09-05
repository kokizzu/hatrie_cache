# Scheduler Monitoring

The monitoring server exposes an authenticated, on-demand scheduler snapshot
at `GET /api/scheduler`. It reports goroutines, `GOMAXPROCS`, visible logical
CPUs, and the corresponding `runtime/metrics` values. It also reports the
number of samples currently represented by the scheduler latency histogram.

The endpoint does not start a polling goroutine, alter runtime settings, or
retain scheduler history. Prometheus exposes the low-cardinality gauges
`hatrie_cache_goroutines`, `hatrie_cache_gomaxprocs`, and
`hatrie_cache_num_cpu` on the existing `/metrics` endpoint.

Go uses goroutines rather than fibers, so the API names the concrete Go
runtime concepts. The exported `hatMonitoring.ReadSchedulerReport` function
provides the same report to embedded users without HTTP.

The report reader is pooled and on-demand. Its benchmark on the local AMD
Ryzen 9 5950X environment is recorded by:

```text
make benchmark-runtime-introspection
```
