package hatSql

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatTrace"
)

type queryTraceSpanExporterProbe struct {
	spans []hatTrace.Span
}

func (probe *queryTraceSpanExporterProbe) Export(_ context.Context, spans []hatTrace.Span) error {
	probe.spans = append([]hatTrace.Span(nil), spans...)
	return nil
}

func TestQueryTraceRecorderExportsThroughSpanExporter(t *testing.T) {
	recorder := NewQueryTraceRecorder(4)
	recorder.ObserveSQLQuery(QueryEvent{
		QueryID:      "query-export",
		ElapsedNanos: 1_000,
		OK:           true,
		Operators:    []QueryOperator{{Node: "SCAN", ElapsedNanos: 500}},
	})
	probe := new(queryTraceSpanExporterProbe)
	if err := recorder.ExportOpenTelemetry(context.Background(), probe); err != nil {
		t.Fatalf("ExportOpenTelemetry() error = %v", err)
	}
	if len(probe.spans) != 2 || probe.spans[0].Name != "hatrie.sql.query" || probe.spans[1].ParentSpanID != probe.spans[0].SpanID {
		t.Fatalf("exported spans = %#v", probe.spans)
	}
	probe.spans[0].Attributes["hatrie.sql.query_id"] = "changed"
	if got := recorder.OpenTelemetrySpans()[0].Attributes["hatrie.sql.query_id"]; got != "query-export" {
		t.Fatalf("export mutated recorder state: %q", got)
	}
}
