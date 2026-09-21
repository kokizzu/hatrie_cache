package hatSql

import (
	"context"
	"testing"
)

func BenchmarkCH230MemoryOvercommit(b *testing.B) {
	query := `FROM VALUES ('b', 2), ('a', 1) AS src(group_id, value)
SELECT src.group_id, SUM(src.value) AS total
GROUP BY src.group_id
ORDER BY src.group_id`
	benchmarks := []struct {
		name    string
		options SQLQueryOptions
	}{
		{name: "default"},
		{name: "overcommit", options: SQLQueryOptions{MemoryOvercommit: mustNewCH230BenchmarkQueue()}},
	}
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				result, err := ExecuteSQLQueryContext(context.Background(), query, nil, benchmark.options)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 2 {
					b.Fatalf("rows = %d, want 2", len(result.Rows))
				}
			}
		})
	}
}

func mustNewCH230BenchmarkQueue() *SQLMemoryOvercommitQueue {
	queue, err := NewSQLMemoryOvercommitQueue(SQLMemoryOvercommitOptions{LimitBytes: 1 << 20})
	if err != nil {
		panic(err)
	}
	return queue
}
