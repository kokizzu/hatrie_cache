package hatSql

import (
	"context"
	"testing"
	"time"
)

var ch033DistributedQueryBenchmarkSink SQLQueryResult

func BenchmarkCH033DistributedQueryBaselineSequential(b *testing.B) {
	shards := ch033BenchmarkShards(8, time.Millisecond)
	query := "FROM CACHE('users') SELECT id"
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		merged := SQLQueryResult{}
		for shardIndex, shard := range shards {
			result, err := ExecuteSQLQueryParameters(context.Background(), query, shard.Resolver, nil, SQLQueryOptions{})
			if err != nil {
				b.Fatal(err)
			}
			if shardIndex == 0 {
				merged.Columns = append([]string(nil), result.Columns...)
			}
			merged.Rows = append(merged.Rows, result.Rows...)
		}
		ch033DistributedQueryBenchmarkSink = merged
	}
}

func BenchmarkCH033DistributedQueryBoundedFanout(b *testing.B) {
	shards := ch033BenchmarkShards(8, time.Millisecond)
	query := "FROM CACHE('users') SELECT id"
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLDistributedQuery(
			context.Background(),
			query,
			shards,
			nil,
			SQLQueryOptions{},
			SQLDistributedQueryOptions{MaxConcurrency: 8},
		)
		if err != nil {
			b.Fatal(err)
		}
		ch033DistributedQueryBenchmarkSink = result
	}
}

func ch033BenchmarkShards(count int, delay time.Duration) []SQLDistributedQueryShard {
	rows := []SQLRow{{"id": int64(1)}}
	resolver := SQLSourceResolverFunc(func(string, string) ([]Row, error) {
		time.Sleep(delay)
		return rows, nil
	})
	shards := make([]SQLDistributedQueryShard, count)
	for index := range shards {
		shards[index] = SQLDistributedQueryShard{ID: "shard-" + string(rune('a'+index)), Resolver: resolver}
	}
	return shards
}
