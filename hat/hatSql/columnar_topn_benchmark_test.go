package hatSql

import (
	"context"
	"testing"
)

type sqlColumnarTopNBenchmarkResolver struct {
	batch ColumnarBatch
	rows  []SQLRow
}

func (resolver sqlColumnarTopNBenchmarkResolver) ResolveSQLSource(string, string) ([]SQLRow, error) {
	return resolver.rows, nil
}

func (resolver sqlColumnarTopNBenchmarkResolver) ResolveSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	return resolver.batch, true, nil
}

type sqlTopNRowsBenchmarkResolver struct{ rows []SQLRow }

func (resolver sqlTopNRowsBenchmarkResolver) ResolveSQLSource(string, string) ([]SQLRow, error) {
	return resolver.rows, nil
}

func BenchmarkExecuteSQLQueryColumnarTopN(b *testing.B) {
	const count = 20_000
	ids := make([]interface{}, count)
	scores := make([]interface{}, count)
	active := make([]interface{}, count)
	rows := make([]SQLRow, count)
	for index := range rows {
		id, score := int64(index), int64((index*7)%count)
		isActive := int64(index % 4)
		ids[index], scores[index], active[index] = id, score, isActive
		rows[index] = SQLRow{"id": id, "score": score, "active": isActive}
	}
	columnar := sqlColumnarTopNBenchmarkResolver{
		batch: ColumnarBatch{Columns: map[string][]interface{}{"id": ids, "score": scores, "active": active}, Rows: count},
		rows:  rows,
	}
	const query = "SELECT id FROM CACHE('items') WHERE active = 1 ORDER BY score DESC LIMIT 50"
	for _, benchmark := range []struct {
		name     string
		resolver SQLSourceResolver
	}{
		{name: "columnar_top_n", resolver: columnar},
		{name: "full_materialized", resolver: sqlTopNRowsBenchmarkResolver{rows: rows}},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := ExecuteSQLQueryParameters(context.Background(), query, benchmark.resolver, nil, SQLQueryOptions{})
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 50 {
					b.Fatalf("rows = %d, want 50", len(result.Rows))
				}
			}
		})
	}
}
