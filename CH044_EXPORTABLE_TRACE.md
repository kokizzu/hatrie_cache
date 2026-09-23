# CH-G44 Exportable Trace Context And Span Exporter

Status: implemented as an opt-in feature.

`hatTrace` now provides a transport-neutral `Span` and `SpanExporter` contract,
plus a bounded OTLP/HTTP JSON exporter. `hatSql.QueryTraceRecorder` can send its
privacy-safe query and operator spans through that contract:

```go
exporter, err := hatTrace.NewOTLPHTTPExporter(hatTrace.OTLPHTTPExporterOptions{
    Endpoint:    "https://collector.example/v1/traces",
    ServiceName: "hatrie-cache",
})
if err != nil {
    return err
}
return recorder.ExportOpenTelemetry(ctx, exporter)
```

The exporter is caller-owned and performs no background work. The zero/default
configuration keeps tracing export disabled. The default limits are 512 spans
per request, 4 MiB encoded payloads, and a 10-second HTTP timeout. The default
client does not follow redirects, and collector response bodies are never
returned in errors. Custom HTTP clients retain their caller-selected transport
and redirect policy.

Only HTTP and HTTPS endpoints are accepted. Headers are cloned at construction,
and the span projection contains query/operator names, IDs, status, timing, and
row counters, not SQL text, predicates, parameters, row values, or error text.

## Measurements

The existing 64-query span projection was measured before and after the change:

| Workload | Before | After | Result |
| --- | --- | --- | --- |
| `OpenTelemetrySpans` | 104,324-118,765 ns/op; 100,974-100,975 B/op; 963 allocs/op | 103,205-116,608 ns/op; 100,974-100,975 B/op; 963 allocs/op | CPU variation is benchmark noise; heap and allocations unchanged |
| OTLP/HTTP export, 32 spans | not applicable | 104,292-146,052 ns/op; 29,891-30,624 B/op; 190-191 allocs/op | Explicit network/export cost; no default-path cost |

The exporter benchmark uses an in-process HTTP test server and does not claim
collector or network latency. Full package verification still exposes the
pre-existing typed-table checkpoint failures in
`m_u05_arrangement_recovery_global_test.go`,
`m_u05_arrangement_recovery_order_test.go`, and
`m_u05_arrangement_recovery_test.go`; focused tests, race, and vet for this
feature pass.

See [QUERY_TRACING.md](QUERY_TRACING.md) for the existing recorder contract.
