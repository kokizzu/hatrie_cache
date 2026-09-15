package hatSql_test

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

type chu05MaterializedExternalResolver struct {
	rows []hatSql.Row
}

func (resolver *chu05MaterializedExternalResolver) ResolveSQLExternalSource(string) ([]hatSql.Row, error) {
	return resolver.rows, nil
}

func (resolver *chu05MaterializedExternalResolver) ResolveSQLSource(string, string) ([]hatSql.Row, error) {
	return resolver.rows, nil
}

func TestCHU05ExternalRunningWindowsUseBoundedStreamingState(t *testing.T) {
	rows := []hatSql.Row{
		{"id": int64(1), "value": int64(1)},
		{"id": int64(2), "value": int64(2)},
		{"id": int64(3), "value": int64(3)},
	}
	resolver := &chu02ExternalStreamResolver{rows: rows}
	var result []hatSql.Row
	err := hatSql.ExecuteSQLQueryRows(context.Background(), `
FROM EXTERNAL('events') AS event
SELECT event.id,
       ROW_NUMBER() OVER () AS row_number,
       SUM(event.value) OVER () AS running_sum,
       LAG(event.value) OVER () AS previous_value`, resolver, nil, hatSql.QueryOptions{}, func(_ []string, row hatSql.SQLRow) error {
		result = append(result, hatSql.Row(row))
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryRows() error = %v", err)
	}
	if resolver.streamCalls != 1 || resolver.materializedCalls != 0 {
		t.Fatalf("resolver calls = stream %d, materialized %d, want stream-only", resolver.streamCalls, resolver.materializedCalls)
	}
	if len(result) != 3 {
		t.Fatalf("result rows = %d, want 3: %#v", len(result), result)
	}
	wantPrevious := []interface{}{nil, int64(1), int64(2)}
	for index, row := range result {
		if row["id"] != int64(index+1) || row["row_number"] != int64(index+1) || row["previous_value"] != wantPrevious[index] {
			t.Fatalf("result[%d] = %#v, want id/row_number=%d and previous=%#v", index, row, index+1, wantPrevious[index])
		}
		if row["running_sum"] != float64(index+1)*(float64(index+2)/2) {
			t.Fatalf("result[%d].running_sum = %#v, want %v", index, row["running_sum"], float64(index+1)*(float64(index+2)/2))
		}
	}
}

func TestCHU05ExternalLeadWindowUsesBoundedStreamingState(t *testing.T) {
	resolver := &chu02ExternalStreamResolver{rows: []hatSql.Row{
		{"id": int64(1), "value": int64(10)},
		{"id": int64(2), "value": int64(20)},
		{"id": int64(3), "value": int64(30)},
	}}
	var result []hatSql.Row
	err := hatSql.ExecuteSQLQueryRows(context.Background(), `
FROM EXTERNAL('events') AS event
SELECT event.id, LEAD(event.value, 1, -1) OVER () AS next_value`, resolver, nil, hatSql.QueryOptions{}, func(_ []string, row hatSql.SQLRow) error {
		result = append(result, hatSql.Row(row))
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryRows() error = %v", err)
	}
	if resolver.streamCalls != 1 || resolver.materializedCalls != 0 {
		t.Fatalf("resolver calls = stream %d, materialized %d, want stream-only", resolver.streamCalls, resolver.materializedCalls)
	}
	want := []interface{}{int64(20), int64(30), int64(-1)}
	if len(result) != len(want) {
		t.Fatalf("result rows = %d, want %d", len(result), len(want))
	}
	for index, expected := range want {
		if result[index]["next_value"] != expected {
			t.Fatalf("result[%d].next_value = %#v, want %#v", index, result[index]["next_value"], expected)
		}
	}
}

func TestCHU05ExternalWindowsRequireStreamingResolver(t *testing.T) {
	resolver := &chu05MaterializedExternalResolver{rows: []hatSql.Row{{"value": int64(1)}}}
	err := hatSql.ExecuteSQLQueryRows(context.Background(), `
FROM EXTERNAL('events') AS event
SELECT ROW_NUMBER() OVER () AS row_number`, resolver, nil, hatSql.QueryOptions{}, func([]string, hatSql.SQLRow) error {
		t.Fatal("stream callback should not run for a materialized-only resolver")
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "cannot stream") {
		t.Fatalf("ExecuteSQLQueryRows() error = %v, want a streaming-source diagnostic", err)
	}
}

func TestCHU05ExternalWindowStreamingMatchesMaterializedRows(t *testing.T) {
	rows := []hatSql.Row{
		{"id": int64(1), "value": int64(4)},
		{"id": int64(2), "value": int64(7)},
		{"id": int64(3), "value": int64(2)},
	}
	query := `
FROM EXTERNAL('events') AS event
SELECT event.id,
       ROW_NUMBER() OVER () AS row_number,
       SUM(event.value) OVER () AS running_sum,
       LAG(event.value, 2, -1) OVER () AS previous_value`
	materialized, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, &chu05MaterializedExternalResolver{rows: rows}, hatSql.QueryOptions{})
	if err != nil {
		t.Fatalf("materialized query error = %v", err)
	}
	var streamed []hatSql.Row
	if err := hatSql.ExecuteSQLQueryRows(context.Background(), query, &chu02ExternalStreamResolver{rows: rows}, nil, hatSql.QueryOptions{}, func(_ []string, row hatSql.SQLRow) error {
		streamed = append(streamed, hatSql.Row(row))
		return nil
	}); err != nil {
		t.Fatalf("streamed query error = %v", err)
	}
	materializedRows := make([]hatSql.Row, len(materialized.Rows))
	for index, row := range materialized.Rows {
		materializedRows[index] = hatSql.Row(row)
	}
	if !reflect.DeepEqual(streamed, materializedRows) {
		t.Fatalf("streamed rows = %#v, materialized rows = %#v", streamed, materializedRows)
	}
}
