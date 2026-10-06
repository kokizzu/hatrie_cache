package hatSql_test

import (
	"context"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestCHU05ExternalPartitionedRunningWindowsUseBoundedStreamingState(t *testing.T) {
	rows := []hatSql.Row{
		{"id": int64(1), "group": "a", "value": int64(1)},
		{"id": int64(2), "group": "a", "value": int64(2)},
		{"id": int64(3), "group": "b", "value": int64(3)},
		{"id": int64(4), "group": "a", "value": int64(4)},
	}
	resolver := &chu02ExternalStreamResolver{rows: rows}
	query := `
FROM EXTERNAL('events') AS event
SELECT event.id, event.group,
       ROW_NUMBER() OVER (PARTITION BY event.group) AS row_number,
       SUM(event.value) OVER (PARTITION BY event.group) AS running_sum,
       LAG(event.value) OVER (PARTITION BY event.group) AS previous_value`
	var result []hatSql.Row
	err := hatSql.ExecuteSQLQueryRows(context.Background(), query, resolver, nil, hatSql.QueryOptions{}, func(_ []string, row hatSql.SQLRow) error {
		result = append(result, hatSql.Row(row))
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryRows() error = %v", err)
	}
	if resolver.streamCalls != 1 || resolver.materializedCalls != 0 {
		t.Fatalf("resolver calls = stream %d, materialized %d, want stream-only", resolver.streamCalls, resolver.materializedCalls)
	}
	want := []hatSql.Row{
		{"id": int64(1), "group": "a", "row_number": int64(1), "running_sum": float64(1), "previous_value": nil},
		{"id": int64(2), "group": "a", "row_number": int64(2), "running_sum": float64(3), "previous_value": int64(1)},
		{"id": int64(3), "group": "b", "row_number": int64(1), "running_sum": float64(3), "previous_value": nil},
		{"id": int64(4), "group": "a", "row_number": int64(3), "running_sum": float64(7), "previous_value": int64(2)},
	}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("partitioned window rows = %#v, want %#v", result, want)
	}
}

func TestCHU05ExternalPartitionedRunningWindowsKeepCompositeAndUnpartitionedStateSeparate(t *testing.T) {
	rows := []hatSql.Row{
		{"id": int64(1), "group": "a", "bucket": "bc", "value": int64(1)},
		{"id": int64(2), "group": "ab", "bucket": "c", "value": int64(2)},
		{"id": int64(3), "group": "a", "bucket": "bc", "value": int64(3)},
	}
	resolver := &chu02ExternalStreamResolver{rows: rows}
	query := `
FROM EXTERNAL('events') AS event
SELECT event.id,
       ROW_NUMBER() OVER (PARTITION BY event.group, event.bucket) AS row_number,
       SUM(event.value) OVER (PARTITION BY event.group, event.bucket) AS running_sum,
       ROW_NUMBER() OVER () AS global_row_number`
	var result []hatSql.Row
	err := hatSql.ExecuteSQLQueryRows(context.Background(), query, resolver, nil, hatSql.QueryOptions{}, func(_ []string, row hatSql.SQLRow) error {
		result = append(result, hatSql.Row(row))
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryRows() error = %v", err)
	}
	if resolver.streamCalls != 1 || resolver.materializedCalls != 0 {
		t.Fatalf("resolver calls = stream %d, materialized %d, want stream-only", resolver.streamCalls, resolver.materializedCalls)
	}
	want := []hatSql.Row{
		{"id": int64(1), "row_number": int64(1), "running_sum": float64(1), "global_row_number": int64(1)},
		{"id": int64(2), "row_number": int64(1), "running_sum": float64(2), "global_row_number": int64(2)},
		{"id": int64(3), "row_number": int64(2), "running_sum": float64(4), "global_row_number": int64(3)},
	}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("composite partitioned window rows = %#v, want %#v", result, want)
	}
}
