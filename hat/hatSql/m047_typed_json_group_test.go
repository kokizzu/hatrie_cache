package hatSql_test

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestM047TypedJSONSubcolumnGroupByCountUsesTypedPath(t *testing.T) {
	column, err := hatSql.NewColumnarJSONSubcolumn([]hatSql.ColumnarJSONSubcolumnValue{
		{Present: true, Value: int64(1)},
		{Present: true, Value: int64(1)},
		{Present: true, Value: int64(2)},
		{Present: false},
		{Present: true, Value: nil},
	})
	if err != nil {
		t.Fatal(err)
	}
	rows := []hatSql.Row{
		{"doc": `{"user":{"id":1}}`},
		{"doc": `{"user":{"id":1}}`},
		{"doc": `{"user":{"id":2}}`},
		{"doc": `{"user":{}}`},
		{"doc": `{"user":{"id":null}}`},
	}
	resolver := &ch031TypedJSONResolver{
		rows:      rows,
		available: true,
		batch: hatSql.ColumnarBatch{
			JSONSubcolumns: map[hatSql.ColumnarJSONSubcolumnKey]hatSql.ColumnarJSONSubcolumn{
				{Field: "doc", Path: "$.user.id"}: column,
			},
			Rows: len(rows),
		},
	}
	query := "SELECT JSON_VALUE(items.doc, '$.user.id') AS id, COUNT(*) AS n FROM CACHE('items') AS items GROUP BY JSON_VALUE(items.doc, '$.user.id')"
	result, err := hatSql.ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := append([]hatSql.SQLRow(nil), result.Rows...)
	sort.SliceStable(got, func(left, right int) bool {
		return fmt.Sprint(got[left]["id"]) < fmt.Sprint(got[right]["id"])
	})
	want := []hatSql.SQLRow{
		{"id": int64(1), "n": int64(2)},
		{"id": int64(2), "n": int64(1)},
		{"id": nil, "n": int64(2)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("typed grouped rows = %#v, want %#v, requests=%#v", got, want, resolver.requests)
	}
	if len(resolver.requests) != 1 || resolver.requests[0].Field != "doc" || resolver.requests[0].Path != "$.user.id" {
		t.Fatalf("typed grouped requested paths = %#v", resolver.requests)
	}
}

func TestM047TypedJSONSubcolumnGroupByCountFallsBackWhenUnavailable(t *testing.T) {
	resolver := &ch031TypedJSONResolver{rows: []hatSql.Row{
		{"doc": `{"user":{"id":1}}`},
		{"doc": `{"user":{"id":1}}`},
		{"doc": `{"user":{"id":2}}`},
	}}
	query := "SELECT JSON_VALUE(doc, '$.user.id') AS id, COUNT(*) AS n FROM CACHE('items') GROUP BY JSON_VALUE(doc, '$.user.id')"
	result, err := hatSql.ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := append([]hatSql.SQLRow(nil), result.Rows...)
	sort.SliceStable(got, func(left, right int) bool {
		return fmt.Sprint(got[left]["id"]) < fmt.Sprint(got[right]["id"])
	})
	want := []hatSql.SQLRow{
		{"id": float64(1), "n": int64(2)},
		{"id": float64(2), "n": int64(1)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("fallback grouped rows = %#v, want %#v", got, want)
	}
}

func TestM047TypedJSONSubcolumnGroupByCountSupportsScalarKinds(t *testing.T) {
	tests := map[string]struct {
		values []hatSql.ColumnarJSONSubcolumnValue
		rows   []hatSql.Row
		want   []hatSql.SQLRow
	}{
		"float": {
			values: []hatSql.ColumnarJSONSubcolumnValue{{Present: true, Value: 1.5}, {Present: true, Value: 1.5}, {Present: true, Value: 2.5}},
			rows:   []hatSql.Row{{"doc": `{"user":{"id":1.5}}`}, {"doc": `{"user":{"id":1.5}}`}, {"doc": `{"user":{"id":2.5}}`}},
			want:   []hatSql.SQLRow{{"id": 1.5, "n": int64(2)}, {"id": 2.5, "n": int64(1)}},
		},
		"string": {
			values: []hatSql.ColumnarJSONSubcolumnValue{{Present: true, Value: "SG"}, {Present: true, Value: "SG"}, {Present: true, Value: "US"}},
			rows:   []hatSql.Row{{"doc": `{"user":{"id":"SG"}}`}, {"doc": `{"user":{"id":"SG"}}`}, {"doc": `{"user":{"id":"US"}}`}},
			want:   []hatSql.SQLRow{{"id": "SG", "n": int64(2)}, {"id": "US", "n": int64(1)}},
		},
		"bool": {
			values: []hatSql.ColumnarJSONSubcolumnValue{{Present: true, Value: true}, {Present: true, Value: true}, {Present: true, Value: false}},
			rows:   []hatSql.Row{{"doc": `{"user":{"id":true}}`}, {"doc": `{"user":{"id":true}}`}, {"doc": `{"user":{"id":false}}`}},
			want:   []hatSql.SQLRow{{"id": true, "n": int64(2)}, {"id": false, "n": int64(1)}},
		},
	}
	for name, testCase := range tests {
		t.Run(name, func(t *testing.T) {
			column, err := hatSql.NewColumnarJSONSubcolumn(testCase.values)
			if err != nil {
				t.Fatal(err)
			}
			resolver := &ch031TypedJSONResolver{
				rows:      testCase.rows,
				available: true,
				batch: hatSql.ColumnarBatch{
					JSONSubcolumns: map[hatSql.ColumnarJSONSubcolumnKey]hatSql.ColumnarJSONSubcolumn{
						{Field: "doc", Path: "$.user.id"}: column,
					},
					Rows: len(testCase.rows),
				},
			}
			result, err := hatSql.ExecuteSQLQueryParameters(context.Background(), "SELECT JSON_VALUE(doc, '$.user.id') AS id, COUNT(*) AS n FROM CACHE('items') GROUP BY JSON_VALUE(doc, '$.user.id')", resolver, nil, hatSql.SQLQueryOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(result.Rows, testCase.want) {
				t.Fatalf("rows = %#v, want %#v", result.Rows, testCase.want)
			}
		})
	}
}

func TestM047TypedJSONSubcolumnGroupByCountFiltersAndEnforcesGroupLimit(t *testing.T) {
	column, err := hatSql.NewColumnarJSONSubcolumn([]hatSql.ColumnarJSONSubcolumnValue{
		{Present: true, Value: int64(1)},
		{Present: true, Value: int64(2)},
		{Present: true, Value: int64(2)},
		{Present: true, Value: int64(3)},
	})
	if err != nil {
		t.Fatal(err)
	}
	resolver := &ch031TypedJSONResolver{
		rows: []hatSql.Row{
			{"doc": `{"user":{"id":1}}`},
			{"doc": `{"user":{"id":2}}`},
			{"doc": `{"user":{"id":2}}`},
			{"doc": `{"user":{"id":3}}`},
		},
		available: true,
		batch: hatSql.ColumnarBatch{
			JSONSubcolumns: map[hatSql.ColumnarJSONSubcolumnKey]hatSql.ColumnarJSONSubcolumn{
				{Field: "doc", Path: "$.user.id"}: column,
			},
			Rows: 4,
		},
	}
	query := "SELECT JSON_VALUE(doc, '$.user.id') AS id, COUNT(*) AS n FROM CACHE('items') WHERE JSON_VALUE(doc, '$.user.id') >= 2 GROUP BY JSON_VALUE(doc, '$.user.id')"
	result, err := hatSql.ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []hatSql.SQLRow{{"id": int64(2), "n": int64(2)}, {"id": int64(3), "n": int64(1)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("filtered rows = %#v, want %#v", result.Rows, want)
	}
	if _, err := hatSql.ExecuteSQLQueryParameters(context.Background(), "SELECT JSON_VALUE(doc, '$.user.id') AS id, COUNT(*) AS n FROM CACHE('items') GROUP BY JSON_VALUE(doc, '$.user.id')", resolver, nil, hatSql.SQLQueryOptions{MaxGroupKeys: 1}); err == nil {
		t.Fatal("MaxGroupKeys unexpectedly allowed the typed grouped query")
	}
}

func BenchmarkM047TypedJSONSubcolumnGroupByCount(b *testing.B) {
	const rowsCount = 10000
	rows := make([]hatSql.Row, rowsCount)
	values := make([]hatSql.ColumnarJSONSubcolumnValue, rowsCount)
	for index := range rowsCount {
		group := int64(index % 128)
		rows[index] = hatSql.Row{"doc": fmt.Sprintf(`{"user":{"id":%d}}`, group)}
		values[index] = hatSql.ColumnarJSONSubcolumnValue{Present: true, Value: group}
	}
	column, err := hatSql.NewColumnarJSONSubcolumn(values)
	if err != nil {
		b.Fatal(err)
	}
	query := "SELECT JSON_VALUE(items.doc, '$.user.id') AS id, COUNT(*) AS n FROM CACHE('items') AS items GROUP BY JSON_VALUE(items.doc, '$.user.id')"

	b.Run("legacy-row-json", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			resolver := &ch031TypedJSONResolver{rows: rows}
			result, err := hatSql.ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, hatSql.SQLQueryOptions{})
			if err != nil || len(result.Rows) != 128 {
				b.Fatalf("legacy result rows=%d err=%v", len(result.Rows), err)
			}
		}
	})

	b.Run("typed-subcolumn", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			resolver := &ch031TypedJSONResolver{
				rows:      rows,
				available: true,
				batch: hatSql.ColumnarBatch{
					JSONSubcolumns: map[hatSql.ColumnarJSONSubcolumnKey]hatSql.ColumnarJSONSubcolumn{
						{Field: "doc", Path: "$.user.id"}: column,
					},
					Rows: rowsCount,
				},
			}
			result, err := hatSql.ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, hatSql.SQLQueryOptions{})
			if err != nil || len(result.Rows) != 128 {
				b.Fatalf("typed result rows=%d err=%v", len(result.Rows), err)
			}
		}
	})
}
