package hatSql

import (
	"context"
	"testing"
)

type sqlColumnarQueryRowsBenchmarkResolver struct {
	rows  []Row
	batch ColumnarBatch
}

func (resolver sqlColumnarQueryRowsBenchmarkResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return resolver.rows, nil
}

func (resolver sqlColumnarQueryRowsBenchmarkResolver) ResolveSQLColumnarSource(_ string, _ string, _ []string) (ColumnarBatch, bool, error) {
	return resolver.batch, true, nil
}

func BenchmarkSQLQueryRowsColumnarSimpleFilter(b *testing.B) {
	const rows = 20_000
	rowSource := make([]Row, rows)
	ids := make([]interface{}, rows)
	scores := make([]interface{}, rows)
	teams := make([]interface{}, rows)
	for index := 0; index < rows; index++ {
		id := int64(index)
		score := int64(index % 100)
		team := "core"
		if index%2 == 0 {
			team = "ops"
		}
		rowSource[index] = Row{"id": id, "score": score, "team": team}
		ids[index], scores[index], teams[index] = id, score, team
	}
	resolver := sqlColumnarQueryRowsBenchmarkResolver{
		rows: rowSource,
		batch: ColumnarBatch{Columns: map[string][]interface{}{
			"id":    ids,
			"score": scores,
			"team":  teams,
		}, Rows: rows},
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		err := ExecuteSQLQueryRows(ctx, "SELECT id, team FROM CACHE('items') WHERE score >= 50", resolver, nil, SQLQueryOptions{}, func([]string, SQLRow) error {
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
