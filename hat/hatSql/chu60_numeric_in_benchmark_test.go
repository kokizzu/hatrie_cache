package hatSql

import (
	"context"
	"testing"
)

var chu60NumericINBenchmarkSink SQLQueryResult

func newCHU60NumericINBenchmarkResolver() chu60NumericINResolver {
	values := make([]interface{}, 16_384)
	for index := range values {
		values[index] = int64(index % 4096)
	}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	batch.PackNumericColumns()
	return chu60NumericINResolver{batch: batch}
}

func BenchmarkCHU60NumericIN(b *testing.B) {
	resolver := newCHU60NumericINBenchmarkResolver()
	query := "SELECT value FROM CACHE('items') WHERE value IN (1000, 2000, 3000, 3500, 3999)"
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		chu60NumericINBenchmarkSink = result
	}
}

func BenchmarkCHU60NumericINOrControl(b *testing.B) {
	resolver := newCHU60NumericINBenchmarkResolver()
	query := "SELECT value FROM CACHE('items') WHERE value = 1000 OR value = 2000 OR value = 3000 OR value = 3500 OR value = 3999"
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		chu60NumericINBenchmarkSink = result
	}
}
