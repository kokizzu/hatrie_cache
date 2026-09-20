package hatSql_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	hatSql "hatrie_cache/hat/hatSql"
)

func TestCHU20ArrayJSONSubcolumnPreservesComplexValuesAndRowAlignment(t *testing.T) {
	documents := []interface{}{
		`{"items":[{"sku":"a"},{"sku":"b"}]}`,
		`{"items":{"count":2}}`,
		`{"other":true}`,
		`{"items":null}`,
	}
	column, err := hatSql.MaterializeJSONSubcolumn("$.items", documents)
	if err != nil {
		t.Fatal(err)
	}
	if column.Kind != hatSql.ColumnarJSONSubcolumnJSON || column.Rows != len(documents) {
		t.Fatalf("column metadata = %#v, want JSON/%d", column, len(documents))
	}
	if len(column.JSONOffsets) != len(documents)+1 || len(column.JSONData) == 0 {
		t.Fatalf("JSON payload layout = offsets %d, data %d", len(column.JSONOffsets), len(column.JSONData))
	}
	if raw, present := column.Value(0); !present || string(raw.(json.RawMessage)) != `[{"sku":"a"},{"sku":"b"}]` {
		t.Fatalf("array Value(0) = %#v, %v", raw, present)
	}
	if raw, present := column.Value(1); !present || string(raw.(json.RawMessage)) != `{"count":2}` {
		t.Fatalf("object Value(1) = %#v, %v", raw, present)
	}
	if value, present := column.Value(2); present || value != nil {
		t.Fatalf("missing Value(2) = %#v, %v", value, present)
	}
	if value, present := column.Value(3); !present || value != nil {
		t.Fatalf("null Value(3) = %#v, %v", value, present)
	}
	for row, want := range []bool{true, true, false, true} {
		if got := column.Exists(row); got != want {
			t.Fatalf("Exists(%d) = %v, want %v", row, got, want)
		}
	}
	for row := 0; row < 2; row++ {
		raw, present := column.Value(row)
		if !present || !json.Valid(raw.(json.RawMessage)) {
			t.Fatalf("stored JSON row %d is invalid: %#v, %v", row, raw, present)
		}
	}

	indexed, err := hatSql.MaterializeJSONSubcolumn("$.items[1].sku", documents[:2])
	if err != nil {
		t.Fatal(err)
	}
	if indexed.Kind != hatSql.ColumnarJSONSubcolumnString {
		t.Fatalf("indexed kind = %v, want string", indexed.Kind)
	}
	if value, present := indexed.Value(0); !present || value != "b" {
		t.Fatalf("indexed Value(0) = %#v, %v", value, present)
	}
}

func TestCHU20ArrayJSONSubcolumnMaterializesOnlyOnJSONQuery(t *testing.T) {
	column, err := hatSql.MaterializeJSONSubcolumn("$.items", []interface{}{
		`{"items":[{"sku":"a"}]}`,
		`{"items":{"count":2}}`,
		`{"other":true}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	resolver := &ch031TypedJSONResolver{available: true, batch: hatSql.ColumnarBatch{
		JSONSubcolumns: map[hatSql.ColumnarJSONSubcolumnKey]hatSql.ColumnarJSONSubcolumn{
			{Field: "doc", Path: "$.items"}: column,
		},
		Rows: 3,
	}}
	result, err := hatSql.ExecuteSQLQueryParameters(context.Background(),
		"SELECT JSON_QUERY(items.doc, '$.items') AS items, JSON_EXISTS(items.doc, '$.items') AS found FROM CACHE('items') AS items",
		resolver, nil, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := []hatSql.SQLRow{
		{"items": []interface{}{map[string]interface{}{"sku": "a"}}, "found": true},
		{"items": map[string]interface{}{"count": float64(2)}, "found": true},
		{"items": nil, "found": false},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}

	_, err = hatSql.ExecuteSQLQueryParameters(context.Background(),
		"SELECT JSON_VALUE(items.doc, '$.items') AS items FROM CACHE('items') AS items",
		resolver, nil, hatSql.SQLQueryOptions{})
	if err == nil || !strings.Contains(err.Error(), "JSON_VALUE requires a scalar path result") {
		t.Fatalf("JSON_VALUE complex result error = %v", err)
	}
}

func TestCHU20ArrayJSONSubcolumnRejectsMalformedRawJSON(t *testing.T) {
	_, err := hatSql.NewColumnarJSONSubcolumnOfKind(hatSql.ColumnarJSONSubcolumnJSON, []hatSql.ColumnarJSONSubcolumnValue{
		{Present: true, Value: json.RawMessage(`{"items":`)},
	})
	if !errors.Is(err, hatSql.ErrColumnarJSONSubcolumnInvalid) {
		t.Fatalf("malformed raw JSON error = %v", err)
	}
}

func TestCHU20AutomaticMaterializerPromotesComplexJSON(t *testing.T) {
	materializer, err := hatSql.NewJSONSubcolumnAutoMaterializer(hatSql.JSONSubcolumnAutoMaterializerOptions{
		MinObservations: 1,
		MaxRows:         4,
		MaxBytes:        1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	column, ready, err := materializer.Observe(hatSql.JSONSubcolumnAutoKey{
		SourceName: "CACHE",
		SourceKey:  "items",
		Field:      "doc",
		Path:       "$.items",
		Generation: 1,
	}, []interface{}{`{"items":[1,2]}`, `{"items":[3]}`})
	if err != nil {
		t.Fatal(err)
	}
	if !ready || column.Kind != hatSql.ColumnarJSONSubcolumnJSON {
		t.Fatalf("promotion = %#v, ready %v; want JSON column", column, ready)
	}
}
