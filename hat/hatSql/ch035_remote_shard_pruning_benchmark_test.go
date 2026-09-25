package hatSql

import (
	"context"
	"testing"
	"time"
)

var ch035RemoteShardPruningBenchmarkSink SQLQueryResult

func BenchmarkCH035RemoteShardPruning(b *testing.B) {
	query := "FROM CACHE('users') SELECT id WHERE region = 'apac'"
	b.Run("all_shards", func(b *testing.B) {
		resolver := SQLSourceResolverFunc(func(string, string) ([]Row, error) {
			time.Sleep(time.Millisecond)
			return []Row{{"id": int64(1), "region": "apac"}}, nil
		})
		shards := make([]SQLDistributedQueryShard, 8)
		for index := range shards {
			shards[index] = SQLDistributedQueryShard{ID: "shard-" + string(rune('a'+index)), Resolver: resolver}
		}
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			result, err := ExecuteSQLDistributedQuery(context.Background(), query, shards, nil, SQLQueryOptions{}, SQLDistributedQueryOptions{MaxConcurrency: 2})
			if err != nil {
				b.Fatal(err)
			}
			ch035RemoteShardPruningBenchmarkSink = result
		}
	})

	b.Run("pruned_7_of_8", func(b *testing.B) {
		shards := make([]SQLDistributedQueryShard, 8)
		for index := range shards {
			shards[index] = SQLDistributedQueryShard{
				ID:       "shard-" + string(rune('a'+index)),
				Resolver: ch035BenchmarkShardResolver{include: index == 0, delay: time.Millisecond},
			}
		}
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			result, err := ExecuteSQLDistributedQuery(context.Background(), query, shards, nil, SQLQueryOptions{}, SQLDistributedQueryOptions{MaxConcurrency: 2})
			if err != nil {
				b.Fatal(err)
			}
			ch035RemoteShardPruningBenchmarkSink = result
		}
	})
}

type ch035BenchmarkShardResolver struct {
	include bool
	delay   time.Duration
}

func (resolver ch035BenchmarkShardResolver) ResolveSQLSource(string, string) ([]Row, error) {
	time.Sleep(resolver.delay)
	return []Row{{"id": int64(1), "region": "apac"}}, nil
}

func (resolver ch035BenchmarkShardResolver) ShouldQuerySQLDistributedShard(string, string, []SQLPartitionPredicate) (bool, bool, error) {
	return resolver.include, true, nil
}
