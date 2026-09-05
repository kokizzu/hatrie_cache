# Query Tracing

`hatSql.QueryTraceRecorder` is an opt-in `QueryObserver` that retains bounded,
privacy-safe query events. `OpenTelemetrySpans()` converts the retained events
to SDK-neutral spans that an application can map to its chosen OpenTelemetry
SDK without adding an OpenTelemetry dependency to this module.

```go
recorder := hatSql.NewQueryTraceRecorder(1024)
result, err := hatSql.ExecuteSQLQueryContext(ctx, query, resolver, hatSql.SQLQueryOptions{
	QueryID:  "orders-by-team",
	Observer: recorder,
})
if err != nil {
	return err
}
_ = result

for _, span := range recorder.OpenTelemetrySpans() {
	// Map span.TraceID, span.SpanID, ParentSpanID, timestamps, Status,
	// Name, and Attributes to the application's OpenTelemetry tracer.
	_ = span
}
```

Each recorded query produces one `hatrie.sql.query` root span and one
`hatrie.sql.operator` child span per observed plan operator. Trace IDs are
stable 32-hex-character values for a retained event, span IDs are
16-character values, and status is `OK` or `ERROR`. Attributes contain query
identity and row/byte counters only; SQL text, predicates, row values, and
error text are never copied into spans.

The existing observer event exposes total query and operator durations but not
phase start offsets. Query and operator spans therefore use the observation
completion time as their end and anchor each reported duration backward from
that point. This preserves measured durations and parentage while remaining
honest about the limits of the existing privacy-safe observer contract.

Tracing is disabled unless an application supplies a recorder as an observer.
The span conversion allocates independent results and attribute maps, so
callers may export or modify them without affecting the recorder. Use a
positive recorder limit in long-running processes.

## Verification

```sh
make test-sql-query-trace-spans
make test-race-sql-query-trace-spans
make benchmark-sql-query-trace-spans
```
