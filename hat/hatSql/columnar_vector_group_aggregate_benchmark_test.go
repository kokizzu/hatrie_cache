package hatSql

import (
	"context"
	"fmt"
	"testing"
)

type sqlVectorRowOnlyResolver struct {
	rows []Row
}

func (resolver sqlVectorRowOnlyResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return resolver.rows, nil
}

func BenchmarkSQLColumnarVectorGroupAggregate(b *testing.B) {
	query := "SELECT region, COUNT(*) AS n, SUM(score) AS total, AVG(score) AS average FROM CACHE('items') GROUP BY region"
	for _, rowCount := range []int{64, 1024, 20000} {
		batch, rows := newSQLVectorBenchmarkInput(rowCount)
		for _, benchmark := range []struct {
			name     string
			columnar bool
		}{
			{name: "baseline_row_executor"},
			{name: "vectorized_columnar", columnar: true},
		} {
			b.Run(fmt.Sprintf("rows_%d/%s", rowCount, benchmark.name), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					var resolver SQLSourceResolver
					if benchmark.columnar {
						resolver = &sqlVectorColumnarResolver{batch: batch, rows: rows}
					} else {
						resolver = sqlVectorRowOnlyResolver{rows: rows}
					}
					result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{})
					if err != nil || len(result.Rows) != minSQLVectorBenchmarkGroups(rowCount) {
						b.Fatalf("query error = %v, rows = %d", err, len(result.Rows))
					}
				}
			})
		}
	}
}

func minSQLVectorBenchmarkGroups(rows int) int {
	if rows < 104 {
		return rows
	}
	return 104
}

func newSQLVectorBenchmarkInput(rows int) (ColumnarBatch, []Row) {
	batch := ColumnarBatch{Columns: map[string][]interface{}{
		"region": make([]interface{}, rows),
		"score":  make([]interface{}, rows),
	}, Rows: rows}
	rowValues := make([]Row, rows)
	for index := 0; index < rows; index++ {
		region := "region-" + string(rune('a'+index%26)) + "-" + string(rune('a'+index/26%4))
		score := int64(index%100 + 1)
		batch.Columns["region"][index] = region
		batch.Columns["score"][index] = score
		rowValues[index] = Row{"region": region, "score": score}
	}
	return batch, rowValues
}
