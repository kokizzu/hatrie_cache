package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type tr017OrderedSourceResolver struct {
	rows []SQLRow
}

func (resolver tr017OrderedSourceResolver) ResolveSQLSource(string, string) ([]SQLRow, error) {
	return nil, errors.New("TR-017 ordered stream must not materialize its source")
}

func (resolver tr017OrderedSourceResolver) StreamSQLOrderedSource(ctx context.Context, _ string, _ string, _ string, _ bool, _ bool, _ bool, visit func(SQLRow) error) (bool, error) {
	for _, row := range resolver.rows {
		if err := ctx.Err(); err != nil {
			return true, err
		}
		if err := visit(row); err != nil {
			return true, err
		}
	}
	return true, nil
}

func TestTR017SingleSourceExecRowUsesScalarFields(t *testing.T) {
	row := newSQLSingleSourceExecRow("src", SQLRow{"id": int64(7)})
	if row.sources != nil || row.order != nil || row.ordinals != nil {
		t.Fatalf("single-source row retained map-backed fields: %#v", row)
	}
	if got := sqlField(row, "src", "id"); got != int64(7) {
		t.Fatalf("qualified scalar field = %#v, want 7", got)
	}
	if got := sqlField(row, "", "id"); got != int64(7) {
		t.Fatalf("unqualified scalar field = %#v, want 7", got)
	}
}

func TestTR017IndexedOrderStreamPreservesProjectionAndPagination(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.id AS id, src.value AS value ORDER BY src.value DESC LIMIT 2 OFFSET 1")
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	resolver := tr017OrderedSourceResolver{rows: []SQLRow{
		{"id": int64(4), "value": int64(40)},
		{"id": int64(3), "value": int64(30)},
		{"id": int64(2), "value": int64(20)},
		{"id": int64(1), "value": int64(10)},
	}}
	result, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("execute ordered stream: %v", err)
	}
	want := []SQLRow{
		{"id": int64(3), "value": int64(30)},
		{"id": int64(2), "value": int64(20)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("ordered stream rows = %#v, want %#v", result.Rows, want)
	}
}
