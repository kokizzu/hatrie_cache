package hatSql

import (
	"context"
	"errors"
)

var (
	// ErrQueryTraceSpanExportContextRequired reports a missing export context.
	ErrQueryTraceSpanExportContextRequired = errors.New("hatSql: query trace export context is required")
	// ErrQueryTraceSpanExporterRequired reports a missing export callback.
	ErrQueryTraceSpanExporterRequired = errors.New("hatSql: query trace span exporter is required")
	// ErrQueryTraceSpanExportBatchSizeInvalid reports an invalid batch bound.
	ErrQueryTraceSpanExportBatchSizeInvalid = errors.New("hatSql: query trace export batch size is invalid")
)

const (
	// DefaultQueryTraceSpanExportBatchSize bounds one callback by default.
	DefaultQueryTraceSpanExportBatchSize = 256
	// MaxQueryTraceSpanExportBatchSize prevents one export callback from
	// receiving an unexpectedly large batch when configuration is untrusted.
	MaxQueryTraceSpanExportBatchSize = 4096
)

// QueryTraceSpanExporter receives one independent batch of SDK-neutral spans.
// Export is synchronous so the callback supplies its own backpressure and
// retry policy without adding a goroutine or queue to query execution.
type QueryTraceSpanExporter func(context.Context, []QueryTraceSpan) error

// QueryTraceSpanExportOptions bounds each exporter callback. A zero BatchSize
// selects DefaultQueryTraceSpanExportBatchSize.
type QueryTraceSpanExportOptions struct {
	BatchSize int
}

// QueryTraceSpanExportStats reports one export attempt. ExportedSpans counts
// only batches whose callback returned nil; a failed batch is included in
// Batches but not ExportedSpans.
type QueryTraceSpanExportStats struct {
	TotalSpans    int
	ExportedSpans int
	Batches       int
}

// ExportOpenTelemetry converts the retained trace snapshot and sends it to an
// application-owned exporter in bounded synchronous batches. The recorder is
// snapshotted before the first callback, so concurrent observations are not
// mixed into the export. A callback error or context cancellation stops the
// export and returns the successfully exported count.
func (recorder *QueryTraceRecorder) ExportOpenTelemetry(ctx context.Context, exporter QueryTraceSpanExporter, options QueryTraceSpanExportOptions) (QueryTraceSpanExportStats, error) {
	if ctx == nil {
		return QueryTraceSpanExportStats{}, ErrQueryTraceSpanExportContextRequired
	}
	if exporter == nil {
		return QueryTraceSpanExportStats{}, ErrQueryTraceSpanExporterRequired
	}
	batchSize := options.BatchSize
	if batchSize == 0 {
		batchSize = DefaultQueryTraceSpanExportBatchSize
	}
	if batchSize < 1 || batchSize > MaxQueryTraceSpanExportBatchSize {
		return QueryTraceSpanExportStats{}, ErrQueryTraceSpanExportBatchSizeInvalid
	}
	if err := ctx.Err(); err != nil {
		return QueryTraceSpanExportStats{}, err
	}
	spans := recorder.OpenTelemetrySpans()
	stats := QueryTraceSpanExportStats{TotalSpans: len(spans)}
	for start := 0; start < len(spans); start += batchSize {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		end := start + batchSize
		if end > len(spans) {
			end = len(spans)
		}
		stats.Batches++
		if err := exporter(ctx, spans[start:end]); err != nil {
			return stats, err
		}
		stats.ExportedSpans += end - start
	}
	return stats, nil
}
