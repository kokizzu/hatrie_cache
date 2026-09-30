package hatSql

import (
	"context"
	"reflect"
	"testing"
)

type chu60NumericINResolver struct {
	batch         ColumnarBatch
	columnarCalls *int
	rowCalls      *int
}

func (resolver chu60NumericINResolver) ResolveSQLSource(string, string) ([]Row, error) {
	if resolver.rowCalls != nil {
		*resolver.rowCalls++
	}
	rows := make([]Row, resolver.batch.Rows)
	for row := range rows {
		value, _ := resolver.batch.Value("value", row)
		rows[row] = Row{"value": value}
	}
	return rows, nil
}

func (resolver chu60NumericINResolver) ResolveSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	if resolver.columnarCalls != nil {
		*resolver.columnarCalls++
	}
	return resolver.batch, true, nil
}

func newCHU60NumericINBatch() ColumnarBatch {
	values := []interface{}{int64(1), int64(2), nil, int64(3), int64(4), int64(5), int64(6)}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	batch.PackNumericColumns()
	return batch
}

func TestCHU60NumericINUsesColumnarMembershipAndPreservesRows(t *testing.T) {
	columnarCalls, rowCalls := 0, 0
	resolver := chu60NumericINResolver{batch: newCHU60NumericINBatch(), columnarCalls: &columnarCalls, rowCalls: &rowCalls}
	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT value FROM CACHE('items') WHERE value IN (2, 4, 4, 6)", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := []SQLRow{{"value": int64(2)}, {"value": int64(4)}, {"value": int64(6)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("numeric IN rows = %#v, want %#v", result.Rows, want)
	}
	if columnarCalls == 0 || rowCalls != 0 {
		t.Fatalf("numeric IN resolver calls = columnar %d, rows %d; want columnar only", columnarCalls, rowCalls)
	}
}

func TestCHU60NumericINMatchesPackedFloatValues(t *testing.T) {
	values := []interface{}{1.5, nil, 2.25, 3.25}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	batch.PackNumericColumns()
	columnarCalls, rowCalls := 0, 0
	resolver := chu60NumericINResolver{batch: batch, columnarCalls: &columnarCalls, rowCalls: &rowCalls}
	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT value FROM CACHE('items') WHERE value IN (3.25, 1.5, 3.25)", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := []SQLRow{{"value": 1.5}, {"value": 3.25}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("float numeric IN rows = %#v, want %#v", result.Rows, want)
	}
	if columnarCalls == 0 || rowCalls != 0 {
		t.Fatalf("float numeric IN resolver calls = columnar %d, rows %d; want columnar only", columnarCalls, rowCalls)
	}
}

func TestCHU60NumericINFallbacksPreserveSQLSemantics(t *testing.T) {
	cases := []struct {
		name  string
		query string
		want  []SQLRow
	}{
		{name: "null list", query: "SELECT value FROM CACHE('items') WHERE value IN (2, NULL, 6)", want: []SQLRow{{"value": int64(2)}, {"value": int64(6)}}},
		{name: "not in", query: "SELECT value FROM CACHE('items') WHERE value NOT IN (2, 4)", want: []SQLRow{{"value": int64(1)}, {"value": int64(3)}, {"value": int64(5)}, {"value": int64(6)}}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			columnarCalls, rowCalls := 0, 0
			resolver := chu60NumericINResolver{batch: newCHU60NumericINBatch(), columnarCalls: &columnarCalls, rowCalls: &rowCalls}
			result, err := ExecuteSQLQueryParameters(context.Background(), test.query, resolver, nil, SQLQueryOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(result.Rows, test.want) {
				t.Fatalf("rows = %#v, want %#v", result.Rows, test.want)
			}
			if columnarCalls != 0 || rowCalls == 0 {
				t.Fatalf("fallback resolver calls = columnar %d, rows %d; want row fallback", columnarCalls, rowCalls)
			}
		})
	}
}
