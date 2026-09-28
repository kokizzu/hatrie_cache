package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type chg001PrewhereResolver struct {
	rows             []Row
	batch            ColumnarBatch
	prewhereFields   []string
	projectionFields []string
	rowIndexes       []int
	normalCalls      int
}

func (resolver *chg001PrewhereResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, errors.New("row source must not be resolved for a prewhere scan")
}

func (resolver *chg001PrewhereResolver) ResolveSQLColumnarSource(_ string, _ string, _ []string) (ColumnarBatch, bool, error) {
	resolver.normalCalls++
	return resolver.batch, true, nil
}

func (resolver *chg001PrewhereResolver) ResolveSQLColumnarPrewhere(_ string, _ string, fields []string) (ColumnarBatch, bool, error) {
	resolver.prewhereFields = append([]string{}, fields...)
	return ColumnarBatch{
		Columns: map[string][]interface{}{
			"score": resolver.batch.Columns["score"],
		},
		Rows: resolver.batch.Rows,
	}, true, nil
}

func (resolver *chg001PrewhereResolver) ResolveSQLColumnarProjection(_ string, _ string, fields []string, rowIndexes []int) (ColumnarBatch, bool, error) {
	resolver.projectionFields = append([]string{}, fields...)
	resolver.rowIndexes = append([]int{}, rowIndexes...)
	columns := make(map[string][]interface{}, len(fields))
	for _, field := range fields {
		values := make([]interface{}, len(rowIndexes))
		for index, rowIndex := range rowIndexes {
			values[index] = resolver.batch.Columns[field][rowIndex]
		}
		columns[field] = values
	}
	return ColumnarBatch{Columns: columns, Rows: len(rowIndexes)}, true, nil
}

func TestSQLColumnarAutomaticPrewhere(t *testing.T) {
	resolver := &chg001PrewhereResolver{batch: ColumnarBatch{
		Columns: map[string][]interface{}{
			"id":      {int64(1), int64(2), int64(3), int64(4), int64(5), int64(6)},
			"score":   {int64(1), int64(5), int64(2), int64(9), int64(3), int64(7)},
			"payload": {"a", "b", "c", "d", "e", "f"},
		},
		Rows: 6,
	}}
	if _, ok := interface{}(resolver).(ColumnarPrewhereSourceResolver); !ok {
		t.Fatal("resolver does not implement prewhere contract")
	}
	var rows []SQLRow
	err := ExecuteSQLQueryRows(context.Background(), "SELECT id, payload FROM CACHE('items') WHERE score >= 5 OFFSET 1 LIMIT 2", resolver, nil, SQLQueryOptions{}, func(_ []string, row SQLRow) error {
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolver.normalCalls != 0 {
		t.Fatalf("legacy columnar calls = %d, want 0", resolver.normalCalls)
	}
	if want := []string{"score"}; !reflect.DeepEqual(resolver.prewhereFields, want) {
		t.Fatalf("prewhere fields = %#v, want %#v", resolver.prewhereFields, want)
	}
	if want := []string{"id", "payload"}; !reflect.DeepEqual(resolver.projectionFields, want) {
		t.Fatalf("projection fields = %#v, want %#v", resolver.projectionFields, want)
	}
	if want := []int{1, 3, 5}; !reflect.DeepEqual(resolver.rowIndexes, want) {
		t.Fatalf("projection row indexes = %#v, want %#v", resolver.rowIndexes, want)
	}
	if want := []SQLRow{{"id": int64(4), "payload": "d"}, {"id": int64(6), "payload": "f"}}; !reflect.DeepEqual(rows, want) {
		t.Fatalf("rows = %#v, want %#v", rows, want)
	}

	materialized := &chg001PrewhereResolver{batch: resolver.batch}
	result, err := ExecuteSQLQuery("SELECT id, payload FROM CACHE('items') WHERE score >= 5 OFFSET 1 LIMIT 2", materialized)
	if err != nil {
		t.Fatal(err)
	}
	if materialized.normalCalls != 0 {
		t.Fatalf("materialized legacy columnar calls = %d, want 0", materialized.normalCalls)
	}
	if want := []SQLRow{{"id": int64(4), "payload": "d"}, {"id": int64(6), "payload": "f"}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("materialized rows = %#v, want %#v", result.Rows, want)
	}
}
