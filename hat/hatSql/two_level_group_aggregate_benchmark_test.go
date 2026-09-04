package hatSql

import (
	"context"
	"testing"
)

func BenchmarkSQLColumnarTwoLevelGroupAggregate(b *testing.B) {
	query := "SELECT group, COUNT(*) AS n, MIN(value) AS low, MAX(value) AS high FROM CACHE('items') GROUP BY group"
	for _, workload := range []struct {
		name     string
		resolver twoLevelColumnarResolver
	}{
		{name: "257_groups", resolver: newTwoLevelColumnarResolver(32 * 1024)},
		{name: "unique_groups", resolver: newTwoLevelColumnarResolverWithGroups(32*1024, 32*1024)},
	} {
		for _, benchmark := range []struct {
			name    string
			options SQLQueryOptions
		}{
			{name: "single_level", options: SQLQueryOptions{}},
			{name: "workers_2", options: SQLQueryOptions{Workers: 2}},
		} {
			b.Run(workload.name+"/"+benchmark.name, func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if _, err := ExecuteSQLQueryContext(context.Background(), query, workload.resolver, benchmark.options); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
