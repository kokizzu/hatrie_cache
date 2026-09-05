package hatSql

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestQueryTraceRecorderExportsOpenTelemetrySpans(t *testing.T) {
	recorder := NewQueryTraceRecorder(4)
	recorder.ObserveSQLQuery(QueryEvent{
		QueryID:      "query-1",
		ElapsedNanos: 5_000,
		OK:           true,
		Operators: []QueryOperator{
			{Node: "SCAN", InputRows: 4, OutputRows: 4, ElapsedNanos: 3_000},
			{Node: "FILTER", InputRows: 4, OutputRows: 2, ElapsedNanos: 1_000},
		},
	})

	spans := recorder.OpenTelemetrySpans()
	if len(spans) != 3 {
		t.Fatalf("OpenTelemetrySpans() returned %d spans, want query plus two operators", len(spans))
	}
	query := spans[0]
	if query.Name != "hatrie.sql.query" || len(query.TraceID) != 32 || len(query.SpanID) != 16 {
		t.Fatalf("query span identity = %#v", query)
	}
	if query.EndUnixNano <= query.StartUnixNano || query.Status != "OK" {
		t.Fatalf("query span timing/status = %#v", query)
	}
	if query.Attributes["hatrie.sql.query_id"] != "query-1" {
		t.Fatalf("query span attributes = %#v", query.Attributes)
	}
	for index, span := range spans[1:] {
		if span.Name != "hatrie.sql.operator" || span.TraceID != query.TraceID || span.ParentSpanID != query.SpanID {
			t.Fatalf("operator span %d identity = %#v, parent=%#v", index, span, query)
		}
		if span.EndUnixNano <= span.StartUnixNano {
			t.Fatalf("operator span %d timing = %#v", index, span)
		}
	}

	spans[0].Attributes["hatrie.sql.query_id"] = "mutated"
	if recorder.OpenTelemetrySpans()[0].Attributes["hatrie.sql.query_id"] != "query-1" {
		t.Fatal("OpenTelemetrySpans() did not return independent attribute maps")
	}

	recorder.ObserveSQLQuery(QueryEvent{QueryID: "query-2", OK: false, Error: "top-secret", Operators: []QueryOperator{{Node: "FILTER"}}})
	spans = recorder.OpenTelemetrySpans()
	if len(spans) != 5 || spans[0].TraceID == spans[3].TraceID || spans[3].Status != "ERROR" {
		t.Fatalf("second query spans = %#v, want independent error trace", spans)
	}
	if spans[4].StartUnixNano != spans[4].EndUnixNano {
		t.Fatalf("zero-duration operator span = %#v, want zero duration", spans[4])
	}

	encoded, err := json.Marshal(spans)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "top-secret") {
		t.Fatalf("span export retained private error text: %s", encoded)
	}
}
