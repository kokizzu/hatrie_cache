package hatSql

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestQueryTraceRecorderBoundsClonesAndExportsJSONL(t *testing.T) {
	recorder := NewQueryTraceRecorder(1)
	recorder.ObserveSQLQuery(QueryEvent{QueryID: "old", Operators: []QueryOperator{{Node: "SCAN", InputRows: 4, OutputRows: 4, ElapsedNanos: 10}}})
	recorder.ObserveSQLQuery(QueryEvent{QueryID: "new", Operators: []QueryOperator{{Node: "FILTER", InputRows: 4, OutputRows: 2, ElapsedNanos: 20}}})

	events := recorder.Events()
	if len(events) != 1 || events[0].QueryID != "new" {
		t.Fatalf("Events() = %#v, want only newest event", events)
	}
	events[0].Operators[0].Node = "changed"
	if got := recorder.Events()[0].Operators[0].Node; got != "FILTER" {
		t.Fatalf("Events() returned aliased operator data: %q", got)
	}

	var output bytes.Buffer
	if err := recorder.WriteJSONL(&output); err != nil {
		t.Fatalf("WriteJSONL() error = %v", err)
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 1 || !strings.Contains(lines[0], `"query_id":"new"`) || !strings.Contains(lines[0], `"node":"FILTER"`) {
		t.Fatalf("WriteJSONL() = %q, want one JSON event", output.String())
	}
}

func TestQueryTraceRecorderNilAndWriterErrors(t *testing.T) {
	var recorder *QueryTraceRecorder
	if got := recorder.Events(); got != nil {
		t.Fatalf("nil Events() = %#v, want nil", got)
	}
	if err := recorder.WriteJSONL(nil); err == nil {
		t.Fatal("nil WriteJSONL() error = nil")
	}
	recorder = NewQueryTraceRecorder(0)
	recorder.ObserveSQLQuery(QueryEvent{QueryID: "writer-error"})
	if err := recorder.WriteJSONL(errorWriter{}); err == nil {
		t.Fatal("WriteJSONL(errorWriter) error = nil")
	}
}

type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) {
	return 0, errors.New("trace test writer failed")
}
