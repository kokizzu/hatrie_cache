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

func TestExecuteSQLQueryUsesColumnarTopNAfterNumericFilter(t *testing.T) {
	resolver := &sqlColumnarQueryRowsResolver{batch: ColumnarBatch{
		Columns: map[string][]interface{}{
			"id":     {int64(1), int64(2), int64(3), int64(4)},
			"score":  {int64(4), int64(9), int64(7), int64(8)},
			"active": {int64(0), int64(1), int64(1), int64(0)},
		},
		Rows: 4,
	}}
	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT id FROM CACHE('items') WHERE active = 1 ORDER BY score DESC LIMIT 2", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	want := []SQLRow{{"id": int64(2)}, {"id": int64(3)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestExecuteSQLQueryUsesColumnarTopNAfterDictionaryFilter(t *testing.T) {
	resolver := &sqlColumnarQueryRowsResolver{batch: ColumnarBatch{
		Columns: map[string][]interface{}{
			"id":    {int64(1), int64(2), int64(3), int64(4)},
			"score": {int64(4), int64(9), int64(7), int64(8)},
		},
		Dictionaries: map[string]DictionaryColumn{
			"state": {Values: []string{"idle", "ready"}, Codes: []uint32{0, 1, 1, 0}},
		},
		Rows: 4,
	}}
	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT id FROM CACHE('items') WHERE state = 'ready' ORDER BY score DESC LIMIT 2", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	want := []SQLRow{{"id": int64(2)}, {"id": int64(3)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestExecuteSQLQueryUsesColumnarTopNAfterDictionaryNumericFilter(t *testing.T) {
	resolver := &sqlColumnarQueryRowsResolver{batch: ColumnarBatch{
		Columns: map[string][]interface{}{
			"id":    {int64(1), int64(2), int64(3), int64(4)},
			"score": {int64(4), int64(9), int64(7), int64(8)},
		},
		Dictionaries: map[string]DictionaryColumn{
			"state": {Values: []string{"idle", "ready"}, Codes: []uint32{0, 1, 1, 0}},
		},
		Rows: 4,
	}}
	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT id FROM CACHE('items') WHERE state = 'ready' AND score >= 8 ORDER BY score DESC LIMIT 2", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	want := []SQLRow{{"id": int64(2)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestExecuteSQLQueryUsesColumnarTopNForDictionaryOrder(t *testing.T) {
	resolver := &sqlColumnarQueryRowsResolver{batch: ColumnarBatch{
		Columns: map[string][]interface{}{"id": {int64(1), int64(2), int64(3), int64(4)}},
		Dictionaries: map[string]DictionaryColumn{
			"team": {Values: []string{"core", "data", "edge"}, Codes: []uint32{1, 0, 2, 0}},
		},
		Rows: 4,
	}}
	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT id FROM CACHE('items') ORDER BY team ASC LIMIT 3", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	want := []SQLRow{{"id": int64(2)}, {"id": int64(4)}, {"id": int64(1)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestExecuteSQLQueryUsesColumnarTopNForMultipleOrderFields(t *testing.T) {
	resolver := &sqlColumnarQueryRowsResolver{batch: ColumnarBatch{
		Columns: map[string][]interface{}{
			"id":    {int64(1), int64(2), int64(3), int64(4)},
			"score": {int64(9), int64(7), int64(5), int64(8)},
		},
		Dictionaries: map[string]DictionaryColumn{
			"team": {Values: []string{"core", "data"}, Codes: []uint32{1, 0, 1, 0}},
		},
		Rows: 4,
	}}
	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT id FROM CACHE('items') ORDER BY team ASC, score DESC LIMIT 3", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	want := []SQLRow{{"id": int64(4)}, {"id": int64(2)}, {"id": int64(1)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}
