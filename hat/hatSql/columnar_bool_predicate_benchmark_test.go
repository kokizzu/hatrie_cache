package hatSql

import (
	"context"
	"errors"
	"testing"
)

type packedBooleanPredicateBenchmarkResolver struct {
	batch ColumnarBatch
}

func (resolver packedBooleanPredicateBenchmarkResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, errors.New("row source must not be resolved for a packed boolean predicate")
}

func (resolver packedBooleanPredicateBenchmarkResolver) ResolveSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	return resolver.batch, true, nil
}

func BenchmarkSQLColumnarPackedBooleanPredicate(b *testing.B) {
	const rows = 4096
	values := make([]interface{}, rows)
	for row := range values {
		values[row] = row&1 == 0
	}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"active": values}, Rows: rows}
	batch.PackBooleanColumns()
	resolver := packedBooleanPredicateBenchmarkResolver{batch: batch}
	query := "FROM CACHE('events') SELECT active WHERE active = true"
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{MaxRows: rows + 1})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != rows/2 {
			b.Fatalf("result rows = %d, want %d", len(result.Rows), rows/2)
		}
	}
}
