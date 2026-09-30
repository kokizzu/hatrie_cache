package hatSql

import (
	"context"
	"reflect"
	"testing"
)

type chu59NumericBetweenResolver struct {
	batch         ColumnarBatch
	columnarCalls *int
}

func (resolver chu59NumericBetweenResolver) ResolveSQLSource(string, string) ([]Row, error) {
	rows := make([]Row, resolver.batch.Rows)
	for row := range rows {
		value, _ := resolver.batch.Value("value", row)
		rows[row] = Row{"value": value}
	}
	return rows, nil
}

func (resolver chu59NumericBetweenResolver) ResolveSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	if resolver.columnarCalls != nil {
		*resolver.columnarCalls++
	}
	return resolver.batch, true, nil
}

func TestCHU59NumericBetweenConvertsToInclusiveNumericBounds(t *testing.T) {
	query, err := parseSQLQueryParameters(`SELECT value FROM CACHE('items') WHERE value BETWEEN 2 AND 4`, nil)
	if err != nil {
		t.Fatal(err)
	}
	predicates, ok := sqlColumnarNumericConjunction(query.where, query.from.alias)
	if !ok {
		t.Fatal("literal numeric BETWEEN was not accepted by the columnar numeric proof")
	}
	want := []sqlColumnarNumericFilter{
		{field: "value", operator: ">=", value: 2},
		{field: "value", operator: "<=", value: 4},
	}
	if !reflect.DeepEqual(predicates, want) {
		t.Fatalf("numeric BETWEEN predicates = %#v, want %#v", predicates, want)
	}
	for _, queryText := range []string{
		"SELECT value FROM CACHE('items') WHERE value NOT BETWEEN 2 AND 4",
		"SELECT value FROM CACHE('items') WHERE value BETWEEN lower AND 4",
		"SELECT value FROM CACHE('items') WHERE value BETWEEN '2' AND '4'",
	} {
		query, err := parseSQLQueryParameters(queryText, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, _, ok := sqlColumnarNumericBetweenPredicate(query.where, query.from.alias); ok {
			t.Fatalf("non-optimized BETWEEN shape was accepted: %s", queryText)
		}
	}
}

func TestCHU59NumericBetweenPreservesBoundariesNullsAndFallbacks(t *testing.T) {
	values := []interface{}{int64(1), int64(2), nil, int64(4), int64(5)}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	batch.PackNumericColumns()
	columnarCalls := 0
	resolver := chu59NumericBetweenResolver{batch: batch, columnarCalls: &columnarCalls}

	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT value FROM CACHE('items') WHERE value BETWEEN 2 AND 4", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if columnarCalls == 0 {
		t.Fatal("BETWEEN query did not resolve a columnar source")
	}
	if want := []SQLRow{{"value": int64(2)}, {"value": int64(4)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("BETWEEN rows = %#v, want %#v", result.Rows, want)
	}

	result, err = ExecuteSQLQueryParameters(context.Background(), "SELECT value FROM CACHE('items') WHERE value BETWEEN 5 AND 2", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("reversed BETWEEN rows = %#v, want empty", result.Rows)
	}

	result, err = ExecuteSQLQueryParameters(context.Background(), "SELECT value FROM CACHE('items') WHERE value NOT BETWEEN 2 AND 4", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []SQLRow{{"value": int64(1)}, {"value": int64(5)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("NOT BETWEEN rows = %#v, want %#v", result.Rows, want)
	}
}
