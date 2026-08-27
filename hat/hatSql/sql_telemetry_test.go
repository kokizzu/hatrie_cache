package hatSql

import (
	"context"
	"strings"
	"testing"
)

func TestSQLTelemetryExportsQueryMetrics(t *testing.T) {
	telemetry := NewSQLTelemetry()
	telemetry.ObserveSQLQuery(SQLQueryEvent{
		ElapsedNanos: 2_000_000,
		ResultBytes:  128,
		OK:           true,
		Operators: []SQLQueryOperator{
			{Node: "INDEX SCAN", InputBytes: intPointer(64), OutputBytes: intPointer(32)},
			{Node: "EXTERNAL SORT", InputBytes: intPointer(32), OutputBytes: intPointer(16)},
		},
	})
	telemetry.ObserveSQLQuery(SQLQueryEvent{ElapsedNanos: 1_000_000, OK: false, Error: "bad query"})

	snapshot := telemetry.Snapshot()
	if snapshot.QueriesTotal != 2 || snapshot.ErrorsTotal != 1 || snapshot.IndexUsesTotal != 1 || snapshot.SpillUsesTotal != 1 || snapshot.ResultBytesTotal != 128 || snapshot.WorkingBytesTotal != 144 {
		t.Fatalf("Snapshot() = %#v", snapshot)
	}
	metrics := telemetry.PrometheusMetrics()
	for _, name := range []string{
		"hatrie_sql_queries_total",
		"hatrie_sql_query_latency_seconds",
		"hatrie_sql_query_index_uses_total",
		"hatrie_sql_query_spill_uses_total",
		"hatrie_sql_query_working_bytes_total",
	} {
		if !strings.Contains(metrics, name) {
			t.Fatalf("PrometheusMetrics() missing %q: %s", name, metrics)
		}
	}
	openTelemetry := telemetry.OpenTelemetryMetrics()
	if len(openTelemetry) == 0 || openTelemetry[0].Name == "" {
		t.Fatalf("OpenTelemetryMetrics() = %#v", openTelemetry)
	}
}

func TestSQLTelemetryObservesExecutedQuery(t *testing.T) {
	telemetry := NewSQLTelemetry()
	if _, err := ExecuteSQLQueryParameters(context.Background(), `SELECT id FROM CACHE('events')`, approximateAggregateSource{{"id": 1}}, nil, SQLQueryOptions{Observer: telemetry}); err != nil {
		t.Fatalf("execute observed query: %v", err)
	}
	if snapshot := telemetry.Snapshot(); snapshot.QueriesTotal != 1 || snapshot.ErrorsTotal != 0 || snapshot.ResultBytesTotal == 0 {
		t.Fatalf("Snapshot() = %#v", snapshot)
	}
}

func intPointer(value int) *int { return &value }
