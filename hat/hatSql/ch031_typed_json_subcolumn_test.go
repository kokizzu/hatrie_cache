package hatSql_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

type ch031TypedJSONResolver struct {
	batch     hatSql.ColumnarBatch
	rows      []hatSql.Row
	available bool
	requests  []hatSql.ColumnarJSONSubcolumnRequest
}

func (resolver *ch031TypedJSONResolver) ResolveSQLSource(string, string) ([]hatSql.Row, error) {
	return resolver.rows, nil
}

func (resolver *ch031TypedJSONResolver) ResolveSQLColumnarSource(string, string, []string) (hatSql.ColumnarBatch, bool, error) {
	return hatSql.ColumnarBatch{}, false, nil
}

func (resolver *ch031TypedJSONResolver) ResolveSQLColumnarJSONSubcolumns(_, _ string, _ []string, paths []hatSql.ColumnarJSONSubcolumnRequest) (hatSql.ColumnarBatch, *hatSql.ColumnarNumericSegments, bool, error) {
	resolver.requests = append([]hatSql.ColumnarJSONSubcolumnRequest(nil), paths...)
	return resolver.batch, nil, resolver.available, nil
}

func TestCH031TypedJSONSubcolumnPreservesMissingAndNull(t *testing.T) {
	column, err := hatSql.NewColumnarJSONSubcolumn([]hatSql.ColumnarJSONSubcolumnValue{
		{Present: true, Value: int64(5)},
		{Present: false},
		{Present: true, Value: nil},
		{Present: true, Value: int64(40)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if column.Kind != hatSql.ColumnarJSONSubcolumnInt64 || column.Rows != 4 {
		t.Fatalf("column metadata = %#v, want int64/4", column)
	}
	if value, present := column.Value(0); !present || value != int64(5) {
		t.Fatalf("Value(0) = %#v, %v", value, present)
	}
	if value, present := column.Value(1); present || value != nil {
		t.Fatalf("missing Value(1) = %#v, %v", value, present)
	}
	if value, present := column.Value(2); !present || value != nil {
		t.Fatalf("null Value(2) = %#v, %v", value, present)
	}

	materialized, err := hatSql.MaterializeJSONSubcolumn("$.user.id", []interface{}{
		`{"user":{"id":1}}`,
		`{"user":{"name":"missing"}}`,
		`{"user":{"id":2.5}}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if materialized.Kind != hatSql.ColumnarJSONSubcolumnFloat64 {
		t.Fatalf("materialized kind = %v, want float64", materialized.Kind)
	}
	if value, present := materialized.Value(0); !present || value != float64(1) {
		t.Fatalf("materialized Value(0) = %#v, %v", value, present)
	}
	if _, present := materialized.Value(1); present {
		t.Fatal("missing materialized path was present")
	}

	for name, testCase := range map[string]struct {
		values []hatSql.ColumnarJSONSubcolumnValue
		kind   hatSql.ColumnarJSONSubcolumnKind
		want   interface{}
	}{
		"float": {
			values: []hatSql.ColumnarJSONSubcolumnValue{{Present: true, Value: int64(1)}, {Present: true, Value: 2.5}},
			kind:   hatSql.ColumnarJSONSubcolumnFloat64,
			want:   float64(2.5),
		},
		"string": {
			values: []hatSql.ColumnarJSONSubcolumnValue{{Present: true, Value: "SG"}, {Present: false}},
			kind:   hatSql.ColumnarJSONSubcolumnString,
			want:   "SG",
		},
		"bool": {
			values: []hatSql.ColumnarJSONSubcolumnValue{{Present: true, Value: true}, {Present: true, Value: false}},
			kind:   hatSql.ColumnarJSONSubcolumnBool,
			want:   true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			column, err := hatSql.NewColumnarJSONSubcolumn(testCase.values)
			if err != nil {
				t.Fatal(err)
			}
			if column.Kind != testCase.kind {
				t.Fatalf("kind = %v, want %v", column.Kind, testCase.kind)
			}
			row := 0
			if name == "float" {
				row = 1
			}
			if value, present := column.Value(row); !present || !reflect.DeepEqual(value, testCase.want) {
				t.Fatalf("Value(%d) = %#v, %v, want %#v", row, value, present, testCase.want)
			}
		})
	}
	if _, err := hatSql.NewColumnarJSONSubcolumn([]hatSql.ColumnarJSONSubcolumnValue{
		{Present: true, Value: "wrong"},
		{Present: true, Value: int64(1)},
	}); !errors.Is(err, hatSql.ErrColumnarJSONSubcolumnInvalid) {
		t.Fatalf("mixed kind error = %v", err)
	}
}

func TestCH031TypedJSONSubcolumnSQLPath(t *testing.T) {
	column, err := hatSql.NewColumnarJSONSubcolumn([]hatSql.ColumnarJSONSubcolumnValue{
		{Present: true, Value: int64(5)},
		{Present: true, Value: int64(10)},
		{Present: false},
		{Present: true, Value: nil},
		{Present: true, Value: int64(40)},
	})
	if err != nil {
		t.Fatal(err)
	}
	resolver := &ch031TypedJSONResolver{available: true, batch: hatSql.ColumnarBatch{
		JSONSubcolumns: map[hatSql.ColumnarJSONSubcolumnKey]hatSql.ColumnarJSONSubcolumn{
			{Field: "doc", Path: "$.user.id"}: column,
		},
		Rows: 5,
	}}
	result, err := hatSql.ExecuteSQLQueryParameters(context.Background(),
		"SELECT JSON_VALUE(items.doc, '$.user.id') AS id, JSON_EXISTS(items.doc, '$.user.id') AS found FROM CACHE('items') AS items",
		resolver, nil, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := []hatSql.SQLRow{
		{"id": int64(5), "found": true},
		{"id": int64(10), "found": true},
		{"id": nil, "found": false},
		{"id": nil, "found": true},
		{"id": int64(40), "found": true},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
	if len(resolver.requests) != 1 || resolver.requests[0].Field != "doc" || resolver.requests[0].Path != "$.user.id" {
		t.Fatalf("requested paths = %#v", resolver.requests)
	}
}

func TestCH031TypedJSONSubcolumnFallsBackWhenUnavailable(t *testing.T) {
	resolver := &ch031TypedJSONResolver{rows: []hatSql.Row{{"doc": `{"user":{"id":7}}`}}}
	result, err := hatSql.ExecuteSQLQueryParameters(context.Background(),
		"SELECT JSON_VALUE(doc, '$.user.id') AS id FROM CACHE('items')",
		resolver, nil, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := []hatSql.SQLRow{{"id": float64(7)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("fallback rows = %#v, want %#v", result.Rows, want)
	}
}
