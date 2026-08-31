package hatSql

import (
	"context"
	"strconv"
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
	teams := make([]string, 20)
	for index := range teams {
		teams[index] = "team-" + strconv.Itoa(index)
	}
	for index := range rows {
		id := int64(index)
		score := int64(index)
		codes[index] = uint32((index * 7) % len(teams))
		ids[index] = id
		scores[index] = score
		rows[index] = SQLRow{"id": id, "score": score, "team": teams[codes[index]]}
	}
	columnar := sqlColumnarTopNBenchmarkResolver{
		batch: ColumnarBatch{Columns: map[string][]interface{}{"id": ids, "score": scores}, Dictionaries: map[string]DictionaryColumn{"team": {Values: teams, Codes: codes}}, Rows: count},
		rows:  rows,
	}
	const query = "SELECT id FROM CACHE('items') WHERE team = 'team-2' AND score >= 10000 ORDER BY score DESC LIMIT 50"
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
