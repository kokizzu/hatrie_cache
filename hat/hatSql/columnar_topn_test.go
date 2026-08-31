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

func TestExecuteSQLQueryUsesNumericSegmentTopNPruningWithoutSkippingTies(t *testing.T) {
	for _, test := range []struct {
		query    string
		scores   []int64
		segments []ColumnarNumericSegment
		want     []SQLRow
	}{
		{
			query:    "SELECT id FROM CACHE('items') ORDER BY score ASC LIMIT 2",
			scores:   []int64{1, 1, 1, 1, 100, 101, 200, 200},
			segments: []ColumnarNumericSegment{{Minimum: 1, Maximum: 1, Valid: true}, {Minimum: 1, Maximum: 1, Valid: true}, {Minimum: 100, Maximum: 101, Valid: true}, {Minimum: 200, Maximum: 200, Valid: true}},
			want:     []SQLRow{{"id": int64(0)}, {"id": int64(1)}},
		},
		{
			query:    "SELECT id FROM CACHE('items') ORDER BY score DESC LIMIT 2",
			scores:   []int64{101, 100, 101, 100, 1, 1, 0, 0},
			segments: []ColumnarNumericSegment{{Minimum: 100, Maximum: 101, Valid: true}, {Minimum: 100, Maximum: 101, Valid: true}, {Minimum: 1, Maximum: 1, Valid: true}, {Minimum: 0, Maximum: 0, Valid: true}},
			want:     []SQLRow{{"id": int64(0)}, {"id": int64(2)}},
		},
	} {
		ids := make([]interface{}, len(test.scores))
		scores := make([]interface{}, len(test.scores))
		for index, score := range test.scores {
			ids[index], scores[index] = int64(index), score
		}
		probe := &sqlSegmentedColumnarSourceProbe{
			batch:    ColumnarBatch{Columns: map[string][]interface{}{"id": ids, "score": scores}, Rows: len(test.scores)},
			segments: &ColumnarNumericSegments{RowsPerSegment: 2, Columns: map[string][]ColumnarNumericSegment{"score": test.segments}},
		}
		result, err := ExecuteSQLQueryParameters(context.Background(), test.query, probe, nil, SQLQueryOptions{})
		if err != nil {
			t.Fatalf("ExecuteSQLQueryParameters(%q) error = %v", test.query, err)
		}
		if !reflect.DeepEqual(result.Rows, test.want) {
			t.Fatalf("ExecuteSQLQueryParameters(%q) rows = %#v, want %#v", test.query, result.Rows, test.want)
		}
		explained, err := ExecuteSQLQueryParameters(context.Background(), "EXPLAIN ANALYZE "+test.query, probe, nil, SQLQueryOptions{})
		if err != nil {
			t.Fatalf("EXPLAIN ANALYZE %q error = %v", test.query, err)
		}
		found := false
		for _, row := range explained.Rows {
			found = found || row["node"] == "COLUMNAR TOP-N SEGMENT SKIP"
		}
		if !found {
			t.Fatalf("EXPLAIN ANALYZE %q rows = %#v, want COLUMNAR TOP-N SEGMENT SKIP", test.query, explained.Rows)
		}
	}
}
