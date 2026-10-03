package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestQueryTraceRecorderExportsSpansInBoundedBatches(t *testing.T) {
	recorder := NewQueryTraceRecorder(4)
	for index := 0; index < 3; index++ {
		recorder.ObserveSQLQuery(QueryEvent{
			QueryID:      "query-" + string(rune('a'+index)),
			ElapsedNanos: 10,
			OK:           true,
			Operators:    []QueryOperator{{Node: "SCAN", ElapsedNanos: 5}},
		})
	}

	var batches [][]QueryTraceSpan
	stats, err := recorder.ExportOpenTelemetry(context.Background(), func(_ context.Context, spans []QueryTraceSpan) error {
		batches = append(batches, spans)
		return nil
	}, QueryTraceSpanExportOptions{BatchSize: 2})
	if err != nil {
		t.Fatalf("ExportOpenTelemetry() error = %v", err)
	}
	if stats.TotalSpans != 6 || stats.ExportedSpans != 6 || stats.Batches != 3 {
		t.Fatalf("export stats = %#v, want six spans in three batches", stats)
	}
	if len(batches) != 3 || len(batches[0]) != 2 || len(batches[1]) != 2 || len(batches[2]) != 2 {
		t.Fatalf("batch sizes = %#v, want [2 2 2]", batches)
	}
	var exported []QueryTraceSpan
	for _, batch := range batches {
		exported = append(exported, batch...)
	}
	if want := recorder.OpenTelemetrySpans(); !reflect.DeepEqual(exported, want) {
		t.Fatalf("exported spans differ from snapshot: got %#v want %#v", exported, want)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	_, err = recorder.ExportOpenTelemetry(ctx, func(context.Context, []QueryTraceSpan) error {
		called = true
		return nil
	}, QueryTraceSpanExportOptions{})
	if !errors.Is(err, context.Canceled) || called {
		t.Fatalf("canceled export = %v, called=%v; want context.Canceled and no callback", err, called)
	}
}

func TestQueryTraceRecorderExportStopsAtExporterError(t *testing.T) {
	recorder := NewQueryTraceRecorder(2)
	recorder.ObserveSQLQuery(QueryEvent{QueryID: "query-a", OK: true})
	recorder.ObserveSQLQuery(QueryEvent{QueryID: "query-b", OK: true})
	wantErr := errors.New("export unavailable")
	stats, err := recorder.ExportOpenTelemetry(context.Background(), func(_ context.Context, _ []QueryTraceSpan) error {
		return wantErr
	}, QueryTraceSpanExportOptions{BatchSize: 1})
	if !errors.Is(err, wantErr) {
		t.Fatalf("export error = %v, want %v", err, wantErr)
	}
	if stats.TotalSpans != 2 || stats.ExportedSpans != 0 || stats.Batches != 1 {
		t.Fatalf("partial export stats = %#v, want total 2/exported 0/batches 1", stats)
	}
}

func TestQueryTraceRecorderExportValidatesInputs(t *testing.T) {
	recorder := NewQueryTraceRecorder(1)
	for _, test := range []struct {
		name     string
		ctx      context.Context
		exporter QueryTraceSpanExporter
		options  QueryTraceSpanExportOptions
		wantErr  error
	}{
		{name: "context", exporter: func(context.Context, []QueryTraceSpan) error { return nil }, wantErr: ErrQueryTraceSpanExportContextRequired},
		{name: "exporter", ctx: context.Background(), wantErr: ErrQueryTraceSpanExporterRequired},
		{name: "batch", ctx: context.Background(), exporter: func(context.Context, []QueryTraceSpan) error { return nil }, options: QueryTraceSpanExportOptions{BatchSize: -1}, wantErr: ErrQueryTraceSpanExportBatchSizeInvalid},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := recorder.ExportOpenTelemetry(test.ctx, test.exporter, test.options)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("ExportOpenTelemetry() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}
