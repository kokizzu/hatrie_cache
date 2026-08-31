package hatSql

import (
	"context"
	"testing"
)

type sqlColumnarDistinctBenchmarkResolver struct {
	rows  []Row
	batch ColumnarBatch
}

func (resolver sqlColumnarDistinctBenchmarkResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return resolver.rows, nil
}

func (resolver sqlColumnarDistinctBenchmarkResolver) ResolveSQLColumnarSource(_ string, _ string, _ []string) (ColumnarBatch, bool, error) {
	return resolver.batch, true, nil
}

func BenchmarkExecuteSQLQueryColumnarDictionaryDistinct(b *testing.B) {
	const rowCount = 20_000
	const groupCount = 20
	rows := make([]Row, rowCount)
	codes := make([]uint32, rowCount)
	values := make([]string, groupCount)
	for group := range values {
		values[group] = "team-" + string(rune('a'+group))
	}
	for rowIndex := range rows {
		team := values[rowIndex%groupCount]
		rows[rowIndex] = Row{"team": team}
		codes[rowIndex] = uint32(rowIndex % groupCount)
	}
	resolver := sqlColumnarDistinctBenchmarkResolver{
		rows: rows,
		batch: ColumnarBatch{
			Dictionaries: map[string]DictionaryColumn{"team": {Values: values, Codes: codes}},
			Rows:         rowCount,
		},
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := ExecuteSQLQueryParameters(ctx, "SELECT DISTINCT team FROM CACHE('items') ORDER BY team", resolver, nil, SQLQueryOptions{})
		if err != nil || len(result.Rows) != groupCount {
			b.Fatalf("execute dictionary distinct: result=%#v err=%v", result, err)
		}
	}
}
