package hatSql

import (
	"context"
	"testing"
)

type sqlColumnarGroupAggregateBenchmarkResolver struct {
	rows  []Row
	batch ColumnarBatch
}

func (resolver sqlColumnarGroupAggregateBenchmarkResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return resolver.rows, nil
}

func (resolver sqlColumnarGroupAggregateBenchmarkResolver) ResolveSQLColumnarSource(_ string, _ string, _ []string) (ColumnarBatch, bool, error) {
	return resolver.batch, true, nil
}

func BenchmarkSQLColumnarDictionaryGroupAggregate(b *testing.B) {
	const rows = 20_000
	const groups = 20
	rowSource := make([]Row, rows)
	scores := make([]interface{}, rows)
	codes := make([]uint32, rows)
	values := make([]string, groups)
	for group := range values {
		values[group] = "team-" + string(rune('a'+group))
	}
	for index := 0; index < rows; index++ {
		team := values[index%groups]
		score := int64(index % 100)
		rowSource[index] = Row{"team": team, "score": score}
		scores[index] = score
		codes[index] = uint32(index % groups)
	}
	resolver := sqlColumnarGroupAggregateBenchmarkResolver{
		rows: rowSource,
		batch: ColumnarBatch{
			Columns:      map[string][]interface{}{"score": scores},
			Dictionaries: map[string]DictionaryColumn{"team": {Values: values, Codes: codes}},
			Rows:         rows,
		},
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		result, err := ExecuteSQLQueryParameters(ctx, "SELECT team, COUNT(*) AS total, SUM(score) AS sum FROM CACHE('items') WHERE score >= 50 GROUP BY team ORDER BY team", resolver, nil, SQLQueryOptions{})
		if err != nil || len(result.Rows) != groups {
			b.Fatalf("execute grouped aggregate: result=%#v err=%v", result, err)
		}
	}
}
