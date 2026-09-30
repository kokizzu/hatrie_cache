package hatSql

import (
	"context"
	"testing"
)

var chu59NumericBetweenBenchmarkSink SQLQueryResult

func newCHU59NumericBetweenBenchmarkResolver(b *testing.B) chu59NumericBetweenResolver {
	values := make([]interface{}, 16_384)
	for index := range values {
		values[index] = int64(index % 4096)
	}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	batch.PackNumericColumns()
	return chu59NumericBetweenResolver{batch: batch}
}

func BenchmarkCHU59NumericBetween(b *testing.B) {
	resolver := newCHU59NumericBetweenBenchmarkResolver(b)
	query := "SELECT value FROM CACHE('items') WHERE value BETWEEN 1000 AND 3000"
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		chu59NumericBetweenBenchmarkSink = result
	}
}

func BenchmarkCHU59NumericBetweenAndControl(b *testing.B) {
	resolver := newCHU59NumericBetweenBenchmarkResolver(b)
	query := "SELECT value FROM CACHE('items') WHERE value >= 1000 AND value <= 3000"
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		chu59NumericBetweenBenchmarkSink = result
	}
}
