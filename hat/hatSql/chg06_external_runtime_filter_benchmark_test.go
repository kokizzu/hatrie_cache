package hatSql_test

import (
	"context"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkCHG06ExternalRuntimeJoinFilter(b *testing.B) {
	left := make([]hatSql.SQLRow, 0, 100000)
	for index := 0; index < 100000; index++ {
		left = append(left, hatSql.SQLRow{"id": index, "k": fmt.Sprintf("key-%06d", index)})
	}
	right := make([]hatSql.SQLRow, 0, 512)
	for index := 0; index < 512; index++ {
		right = append(right, hatSql.SQLRow{"id": 1000000 + index, "k": fmt.Sprintf("key-%06d", index)})
	}
	query := "FROM EXTERNAL('left') AS l JOIN EXTERNAL('right') AS r ON l.k = r.k SELECT l.id, r.id AS right_id"

	for _, benchmark := range []struct {
		name    string
		options hatSql.QueryOptions
	}{
		{name: "materialized", options: hatSql.QueryOptions{}},
		{name: "runtime_filter", options: hatSql.QueryOptions{RuntimeJoinBloomFilter: true}},
	} {
		benchmark := benchmark
		b.Run(benchmark.name, func(b *testing.B) {
			resolver := &runtimeExternalJoinFilterResolver{sources: map[string][]hatSql.SQLRow{
				"left":  left,
				"right": right,
			}}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, benchmark.options)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 512 {
					b.Fatalf("result rows = %d, want 512", len(result.Rows))
				}
			}
		})
	}
}
