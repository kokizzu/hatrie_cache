package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type sqlColumnarDistinctResolver struct {
	batch ColumnarBatch
	calls int
}

func (resolver *sqlColumnarDistinctResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, errors.New("row source must not be resolved for a columnar dictionary distinct")
}

func (resolver *sqlColumnarDistinctResolver) ResolveSQLColumnarSource(_ string, _ string, _ []string) (ColumnarBatch, bool, error) {
	resolver.calls++
	return resolver.batch, true, nil
}

func TestExecuteSQLQueryUsesColumnarDictionaryDistinct(t *testing.T) {
	resolver := &sqlColumnarDistinctResolver{batch: ColumnarBatch{
		Dictionaries: map[string]DictionaryColumn{
			"team": {Values: []string{"ops", "core", "data"}, Codes: []uint32{0, 1, 2, 1}},
		},
		Rows: 4,
	}}
	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT DISTINCT team FROM CACHE('items') ORDER BY team", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resolver.calls != 1 {
		t.Fatalf("columnar calls = %d, want 1", resolver.calls)
	}
	want := []SQLRow{{"team": "core"}, {"team": "data"}, {"team": "ops"}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestExecuteSQLQueryUsesColumnarDictionaryDistinctPage(t *testing.T) {
	resolver := &sqlColumnarDistinctResolver{batch: ColumnarBatch{
		Dictionaries: map[string]DictionaryColumn{
			"team": {Values: []string{"ops", "core", "data"}, Codes: []uint32{0, 1, 2, 1}},
		},
		Rows: 4,
	}}
	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT DISTINCT team FROM CACHE('items') ORDER BY team DESC LIMIT 1 OFFSET 1", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resolver.calls != 1 {
		t.Fatalf("columnar calls = %d, want 1", resolver.calls)
	}
	want := []SQLRow{{"team": "data"}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestExecuteSQLQueryUsesColumnarDictionaryDistinctAfterNumericFilter(t *testing.T) {
	resolver := &sqlColumnarDistinctResolver{batch: ColumnarBatch{
		Columns: map[string][]interface{}{
			"score": {int64(7), int64(10), int64(12), int64(20)},
		},
		Dictionaries: map[string]DictionaryColumn{
			"team": {Values: []string{"ops", "core", "data"}, Codes: []uint32{0, 1, 2, 1}},
		},
		Rows: 4,
	}}
	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT DISTINCT team FROM CACHE('items') WHERE score >= 10 ORDER BY team", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resolver.calls != 1 {
		t.Fatalf("columnar calls = %d, want 1", resolver.calls)
	}
	want := []SQLRow{{"team": "core"}, {"team": "data"}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}
