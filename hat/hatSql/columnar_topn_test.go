package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestExecuteSQLQueryUsesColumnarTopN(t *testing.T) {
	resolver := &sqlColumnarQueryRowsResolver{batch: ColumnarBatch{
		Columns: map[string][]interface{}{
			"id":    {int64(1), int64(2), int64(3), int64(4)},
			"score": {int64(4), int64(9), int64(7), int64(8)},
		},
		Rows: 4,
	}}
	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT id FROM CACHE('items') ORDER BY score DESC LIMIT 2", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	if resolver.columnarCalls != 1 {
		t.Fatalf("columnar calls = %d, want 1", resolver.columnarCalls)
	}
	want := []SQLRow{{"id": int64(2)}, {"id": int64(4)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestExecuteSQLQueryColumnarTopNOffsetPastResult(t *testing.T) {
	resolver := &sqlColumnarQueryRowsResolver{batch: ColumnarBatch{
		Columns: map[string][]interface{}{
			"id":    {int64(1), int64(2)},
			"score": {int64(4), int64(9)},
		},
		Rows: 2,
	}}
	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT id FROM CACHE('items') ORDER BY score DESC LIMIT 1 OFFSET 3", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("rows = %#v, want empty page", result.Rows)
	}
}
