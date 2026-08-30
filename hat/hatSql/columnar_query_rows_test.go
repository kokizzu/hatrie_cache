package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type sqlColumnarQueryRowsResolver struct {
	batch           ColumnarBatch
	columnarCalls   int
	requestedFields []string
}

func (resolver *sqlColumnarQueryRowsResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, errors.New("row source must not be resolved for a columnar query rows scan")
}

func (resolver *sqlColumnarQueryRowsResolver) ResolveSQLColumnarSource(_ string, _ string, fields []string) (ColumnarBatch, bool, error) {
	resolver.columnarCalls++
	resolver.requestedFields = append([]string{}, fields...)
	return resolver.batch, true, nil
}

func TestExecuteSQLQueryRowsStreamsColumnarSource(t *testing.T) {
	resolver := &sqlColumnarQueryRowsResolver{batch: ColumnarBatch{
		Columns: map[string][]interface{}{
			"id":    {int64(1), int64(2), int64(3)},
			"score": {int64(9), int64(10), int64(12)},
			"team":  {"ops", "core", "data"},
		},
		Rows: 3,
	}}
	var columns []string
	var rows []SQLRow
	err := ExecuteSQLQueryRows(context.Background(), "SELECT id, team FROM CACHE('items') WHERE score >= 10 OFFSET 1 LIMIT 1", resolver, nil, SQLQueryOptions{}, func(gotColumns []string, row SQLRow) error {
		columns = append([]string{}, gotColumns...)
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolver.columnarCalls != 1 {
		t.Fatalf("columnar calls = %d, want 1", resolver.columnarCalls)
	}
	if want := []string{"score", "id", "team"}; !reflect.DeepEqual(resolver.requestedFields, want) {
		t.Fatalf("requested fields = %#v, want %#v", resolver.requestedFields, want)
	}
	if want := []string{"id", "team"}; !reflect.DeepEqual(columns, want) {
		t.Fatalf("columns = %#v, want %#v", columns, want)
	}
	if want := []SQLRow{{"id": int64(3), "team": "data"}}; !reflect.DeepEqual(rows, want) {
		t.Fatalf("rows = %#v, want %#v", rows, want)
	}
}
