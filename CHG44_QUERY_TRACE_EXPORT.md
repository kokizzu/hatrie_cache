# CH-G44 Query Trace Span Export

`hatSql.QueryTraceRecorder` already exposes privacy-safe SDK-neutral
OpenTelemetry-compatible spans. This addition provides an opt-in synchronous
export callback without importing an OpenTelemetry SDK or starting a worker.

```go
stats, err := recorder.ExportOpenTelemetry(
	ctx,
	func(ctx context.Context, spans []hatSql.QueryTraceSpan) error {
		return applicationExporter.Export(ctx, spans)
	},
	hatSql.QueryTraceSpanExportOptions{BatchSize: 128},
)
```

The callback receives independent bounded batches in recorder order. A zero
batch size uses `DefaultQueryTraceSpanExportBatchSize` (256); configured sizes
are capped at `MaxQueryTraceSpanExportBatchSize` (4096). Cancellation is
checked before the snapshot and before every callback. A callback error stops
the export and the returned `QueryTraceSpanExportStats` reports the number of
successfully exported spans.

The exporter is deliberately application-owned: authentication, OTLP client
selection, retries, queueing, and remote backpressure remain outside this
package. No observer or exporter is installed by default, so ordinary query
execution has no new exporter allocation or goroutine. The package continues
to export only the existing privacy-safe span projection, not raw SQL or error
text.

This is a snapshot export, not a durable cursor. A caller that retries after an
error may receive already-exported spans again and must use its own idempotency
or checkpoint policy.
