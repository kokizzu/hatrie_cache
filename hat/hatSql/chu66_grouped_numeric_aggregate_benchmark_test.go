package hatSql

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkSQLColumnarGroupedNumericAggregates(b *testing.B) {
	const rows = 100000
	const groups = 64
	values := make([]float64, rows)
	valid := make([]bool, rows)
	codes := make([]uint32, rows)
	groupNames := make([]string, groups)
	for index := range groupNames {
		groupNames[index] = fmt.Sprintf("group-%02d", index)
	}
	for index := range values {
		values[index] = float64(index%1000) + 0.5
		valid[index] = index%5 != 0
		codes[index] = uint32(index % groups)
	}
	resolver := chu66GroupedNumericAggregateResolver{batch: chu66GroupedBatch(values, valid, codes, groupNames)}
	query := "SELECT group, SUM(value) AS sum, AVG(value) AS avg, MIN(value) AS min, MAX(value) AS max FROM CACHE('items') GROUP BY group ORDER BY group"
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != groups || result.Rows[0]["group"] != "group-00" {
			b.Fatalf("grouped aggregate result rows = %d, first = %#v", len(result.Rows), result.Rows)
		}
	}
}
