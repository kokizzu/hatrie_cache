package hatSql_test

import (
	"context"
	"strconv"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func newM046JSONTopNResolver(rows int, available bool) *ch031TypedJSONResolver {
	values := make([]hatSql.ColumnarJSONSubcolumnValue, rows)
	sourceRows := make([]hatSql.Row, rows)
	for row := range values {
		value := int64(row)
		values[row] = hatSql.ColumnarJSONSubcolumnValue{Present: true, Value: value}
		sourceRows[row] = hatSql.Row{"doc": `{"user":{"id":` + strconv.FormatInt(value, 10) + `}}`}
	}
	column, err := hatSql.NewColumnarJSONSubcolumn(values)
	if err != nil {
		panic(err)
	}
	return &ch031TypedJSONResolver{
		rows:      sourceRows,
		available: available,
		batch: hatSql.ColumnarBatch{
			JSONSubcolumns: map[hatSql.ColumnarJSONSubcolumnKey]hatSql.ColumnarJSONSubcolumn{
				{Field: "doc", Path: "$.user.id"}: column,
			},
			Rows: rows,
		},
	}
}

func BenchmarkM046JSONSubcolumnTopN(b *testing.B) {
	const rows = 4096
	query := "SELECT JSON_VALUE(items.doc, '$.user.id') AS id FROM CACHE('items') AS items ORDER BY JSON_VALUE(items.doc, '$.user.id') DESC LIMIT 32"
	for _, testCase := range []struct {
		name      string
		available bool
	}{
		{name: "legacy", available: false},
		{name: "candidate", available: true},
	} {
		b.Run(testCase.name, func(b *testing.B) {
			resolver := newM046JSONTopNResolver(rows, testCase.available)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				result, err := hatSql.ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, hatSql.SQLQueryOptions{})
				if err != nil || len(result.Rows) != 32 {
					b.Fatalf("result rows = %d, err = %v", len(result.Rows), err)
				}
			}
		})
	}
}
