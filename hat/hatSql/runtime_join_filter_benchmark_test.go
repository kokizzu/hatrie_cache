package hatSql_test

import (
	"context"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkSQLRuntimeJoinFilter(b *testing.B) {
	cases := []struct {
		name       string
		leftRows   int
		rightRows  int
		hotKey     bool
		resultRows int
	}{
		{name: "selective_100k_left_512_right", leftRows: 100000, rightRows: 512, resultRows: 512},
		{name: "balanced_1k_left_1k_right", leftRows: 1024, rightRows: 1024, resultRows: 1024},
		{name: "hot_key_100k_left_1_right", leftRows: 100000, rightRows: 1, hotKey: true, resultRows: 100000},
	}
	for _, benchmark := range cases {
		benchmark := benchmark
		b.Run(benchmark.name+"/baseline", func(b *testing.B) {
			benchmarkSQLRuntimeJoinFilter(b, benchmark, hatSql.QueryOptions{})
		})
		b.Run(benchmark.name+"/runtime_filter", func(b *testing.B) {
			benchmarkSQLRuntimeJoinFilter(b, benchmark, hatSql.QueryOptions{RuntimeJoinBloomFilter: true})
		})
	}
}

func benchmarkSQLRuntimeJoinFilter(b *testing.B, benchmark struct {
	name       string
	leftRows   int
	rightRows  int
	hotKey     bool
	resultRows int
}, options hatSql.QueryOptions) {
	b.Helper()
	resolver := newRuntimeJoinFilterBenchmarkResolver(benchmark.leftRows, benchmark.rightRows, benchmark.hotKey)
	query := "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.id AS right_id"
	b.ReportAllocs()
	for b.Loop() {
		result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, options)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != benchmark.resultRows {
			b.Fatalf("result rows = %d, want %d", len(result.Rows), benchmark.resultRows)
		}
	}
}

func newRuntimeJoinFilterBenchmarkResolver(leftRows, rightRows int, hotKey bool) *runtimeJoinFilterResolver {
	left := make([]hatSql.SQLRow, 0, leftRows)
	for index := 0; index < leftRows; index++ {
		key := fmt.Sprintf("probe-%06d", index)
		if hotKey {
			key = "hot"
		}
		left = append(left, hatSql.SQLRow{"id": index, "k": key})
	}
	right := make([]hatSql.SQLRow, 0, rightRows)
	for index := 0; index < rightRows; index++ {
		key := fmt.Sprintf("probe-%06d", index)
		if hotKey {
			key = "hot"
		}
		right = append(right, hatSql.SQLRow{"id": 1000000 + index, "k": key})
	}
	return &runtimeJoinFilterResolver{sources: map[string][]hatSql.SQLRow{"left": left, "right": right}}
}
