package hatSql

import (
	"context"
	"strconv"
	"testing"
)

func BenchmarkExecuteSQLQueryColumnarTopNMultiOrder(b *testing.B) {
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
		score := int64((index * 13) % 10_000)
		codes[index] = uint32((index * 7) % len(teams))
		ids[index], scores[index] = id, score
		rows[index] = SQLRow{"id": id, "score": score, "team": teams[codes[index]]}
	}
	columnar := sqlColumnarTopNBenchmarkResolver{
		batch: ColumnarBatch{Columns: map[string][]interface{}{"id": ids, "score": scores}, Dictionaries: map[string]DictionaryColumn{"team": {Values: teams, Codes: codes}}, Rows: count},
		rows:  rows,
	}
	const query = "SELECT id FROM CACHE('items') ORDER BY team ASC, score DESC LIMIT 50"
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
			for b.Loop() {
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
