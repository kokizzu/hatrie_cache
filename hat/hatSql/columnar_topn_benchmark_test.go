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
	rows := make([]SQLRow, count)
	codes := make([]uint32, count)
	for index := range rows {
		id, score := int64(index), int64((index*7)%count)
		state := "idle"
		if index%4 == 1 {
			state, codes[index] = "ready", 1
		}
		ids[index], scores[index] = id, score
		rows[index] = SQLRow{"id": id, "score": score, "state": state}
	}
	columnar := sqlColumnarTopNBenchmarkResolver{
		batch: ColumnarBatch{Columns: map[string][]interface{}{"id": ids, "score": scores}, Dictionaries: map[string]DictionaryColumn{"state": {Values: []string{"idle", "ready"}, Codes: codes}}, Rows: count},
		rows:  rows,
	}
	const query = "SELECT id FROM CACHE('items') WHERE state = 'ready' ORDER BY score DESC LIMIT 50"
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
