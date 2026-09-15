package hatSql

import (
	"context"
	"strconv"
	"testing"
)

type ch031TypedJSONBenchmarkResolver struct {
	batch ColumnarBatch
}

func (resolver ch031TypedJSONBenchmarkResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, nil
}

func (resolver ch031TypedJSONBenchmarkResolver) ResolveSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	return ColumnarBatch{}, false, nil
}

func (resolver ch031TypedJSONBenchmarkResolver) ResolveSQLColumnarJSONSubcolumns(_, _ string, _ []string, _ []ColumnarJSONSubcolumnRequest) (ColumnarBatch, *ColumnarNumericSegments, bool, error) {
	return resolver.batch, nil, true, nil
}

func newCH031TypedJSONBenchmarkResolver(rows int) ch031TypedJSONBenchmarkResolver {
	values := make([]ColumnarJSONSubcolumnValue, rows)
	for row := range values {
		values[row] = ColumnarJSONSubcolumnValue{Present: true, Value: int64(row)}
	}
	column, err := NewColumnarJSONSubcolumn(values)
	if err != nil {
		panic(err)
	}
	return ch031TypedJSONBenchmarkResolver{batch: ColumnarBatch{
		JSONSubcolumns: map[ColumnarJSONSubcolumnKey]ColumnarJSONSubcolumn{
			{Field: "doc", Path: "$.id"}: column,
		},
		Rows: rows,
	}}
}

func ch031TypedJSONSubcolumnBytes(column ColumnarJSONSubcolumn) int {
	return len(column.Int64)*8 + len(column.Float64)*8 + len(column.Strings)*16 + len(column.BoolBits) + len(column.Present) + len(column.Validity)
}

func BenchmarkCH031JSONValueTypedSubcolumn(b *testing.B) {
	resolver := newCH031TypedJSONBenchmarkResolver(4096)
	column := resolver.batch.JSONSubcolumns[ColumnarJSONSubcolumnKey{Field: "doc", Path: "$.id"}]
	query := "SELECT JSON_VALUE(doc, '$.id') AS id FROM CACHE('items') WHERE JSON_VALUE(doc, '$.id') >= 2048"
	b.ReportMetric(float64(ch031TypedJSONSubcolumnBytes(column)), "subcolumn_bytes")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{})
		if err != nil || len(result.Rows) != 2048 {
			b.Fatalf("typed result = %d rows, err=%v", len(result.Rows), err)
		}
	}
}

func BenchmarkCH031JSONSubcolumnMaterialize(b *testing.B) {
	documents := make([]interface{}, 4096)
	for row := range documents {
		documents[row] = `{"id":` + strconv.Itoa(row) + `}`
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		column, err := MaterializeJSONSubcolumn("$.id", documents)
		if err != nil || column.Rows != len(documents) {
			b.Fatalf("materialize rows = %d, err=%v", column.Rows, err)
		}
	}
}
