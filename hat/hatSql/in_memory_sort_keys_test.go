package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestSQLSortQueryOutputsWithKeysStable(t *testing.T) {
	outputs := []sqlQueryOutput{
		{row: SQLRow{"id": int64(1)}},
		{row: SQLRow{"id": int64(2)}},
		{row: SQLRow{"id": int64(3)}},
	}
	records := []sqlSpillOutput{
		{Keys: []interface{}{int64(2)}, Ordinal: 0},
		{Keys: []interface{}{int64(1)}, Ordinal: 1},
		{Keys: []interface{}{int64(1)}, Ordinal: 2},
	}

	sqlSortQueryOutputsWithKeys(outputs, records, []sqlOrder{{}})

	want := []int64{2, 3, 1}
	for index, row := range outputs {
		if got := row.row["id"]; got != want[index] {
			t.Fatalf("outputs[%d].id = %#v, want %d", index, got, want[index])
		}
	}
}

func TestSQLLimitWithTiesComputedOrderUsesMaterializedKeys(t *testing.T) {
	query := "FROM VALUES (1, 10), (2, 9), (3, 10) AS items(id, score) SELECT items.id, items.score ORDER BY items.score + 1 DESC LIMIT 1 WITH TIES"
	result, err := ExecuteSQLQueryContext(context.Background(), query, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := []SQLRow{
		{"id": int64(1), "score": int64(10)},
		{"id": int64(3), "score": int64(10)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLLimitWithTiesGroupedOrderPreservesGroupState(t *testing.T) {
	query := "FROM VALUES (1, 'a'), (2, 'a'), (3, 'b') AS items(id, category) SELECT items.category, COUNT(*) AS total GROUP BY items.category ORDER BY COUNT(*) DESC LIMIT 1 WITH TIES"
	result, err := ExecuteSQLQueryContext(context.Background(), query, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := []SQLRow{
		{"category": "a", "total": int64(2)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}
