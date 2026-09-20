package hatSql

import (
	"context"
	"testing"
)

type chu20BenchmarkResolver struct {
	rows      []Row
	batch     ColumnarBatch
	available bool
}

func (resolver *chu20BenchmarkResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return resolver.rows, nil
}

func (*chu20BenchmarkResolver) ResolveSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	return ColumnarBatch{}, false, nil
}

func (resolver *chu20BenchmarkResolver) ResolveSQLColumnarJSONSubcolumns(string, string, []string, []ColumnarJSONSubcolumnRequest) (ColumnarBatch, *ColumnarNumericSegments, bool, error) {
	return resolver.batch, nil, resolver.available, nil
}

func BenchmarkCHU20JSONQueryRowSource(b *testing.B) {
	rows := make([]Row, 1024)
	for row := range rows {
		rows[row] = Row{"doc": `{"user":{"id":` + testJSONInt(row) + `,"roles":["admin","ops"],"active":true}}`}
	}
	query := "SELECT JSON_QUERY(doc, '$.user') AS user FROM CACHE('items')"
	b.Run("row_source", func(b *testing.B) {
		resolver := &chu20BenchmarkResolver{rows: rows}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{})
			if err != nil || len(result.Rows) != len(rows) {
				b.Fatalf("query rows = %d, err = %v", len(result.Rows), err)
			}
		}
	})

	column, err := MaterializeJSONSubcolumn("$.user", makeBenchmarkDocuments(rows))
	if err != nil {
		b.Fatal(err)
	}
	b.Run("packed_subcolumn", func(b *testing.B) {
		resolver := &chu20BenchmarkResolver{
			rows: rows,
			batch: ColumnarBatch{
				Rows: len(rows),
				JSONSubcolumns: map[ColumnarJSONSubcolumnKey]ColumnarJSONSubcolumn{
					{Field: "doc", Path: "$.user"}: column,
				},
			},
			available: true,
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{})
			if err != nil || len(result.Rows) != len(rows) {
				b.Fatalf("query rows = %d, err = %v", len(result.Rows), err)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(columnarJSONSubcolumnRetainedBytes(column)), "retained-B/op")
	})
	b.Run("materialize", func(b *testing.B) {
		documents := makeBenchmarkDocuments(rows)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			column, err := MaterializeJSONSubcolumn("$.user", documents)
			if err != nil || column.Rows != len(rows) {
				b.Fatalf("materialized rows = %d, err = %v", column.Rows, err)
			}
		}
	})
}

func makeBenchmarkDocuments(rows []Row) []interface{} {
	documents := make([]interface{}, len(rows))
	for row, value := range rows {
		documents[row] = value["doc"]
	}
	return documents
}

func testJSONInt(value int) string {
	if value == 0 {
		return "0"
	}
	var digits [20]byte
	index := len(digits)
	for value > 0 {
		index--
		digits[index] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[index:])
}
