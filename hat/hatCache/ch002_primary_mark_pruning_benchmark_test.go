package hatCache

import (
	"context"
	"testing"
)

type ch002HatTrieLegacyResolver struct {
	trie *HatTrie
}

func (resolver ch002HatTrieLegacyResolver) ResolveSQLSource(name, key string) ([]SQLRow, error) {
	return resolver.trie.ResolveSQLSource(name, key)
}

func (resolver ch002HatTrieLegacyResolver) StreamSQLSource(ctx context.Context, name, key string, visit func(SQLRow) error) error {
	return resolver.trie.StreamSQLSource(ctx, name, key, visit)
}

func (resolver ch002HatTrieLegacyResolver) ResolveSQLOrderedSource(name, key, field string, desc, nullsFirst, nullsLast bool) ([]SQLRow, bool, error) {
	return resolver.trie.ResolveSQLOrderedSource(name, key, field, desc, nullsFirst, nullsLast)
}

func (resolver ch002HatTrieLegacyResolver) StreamSQLOrderedSource(ctx context.Context, name, key, field string, desc, nullsFirst, nullsLast bool, visit func(SQLRow) error) (bool, error) {
	return resolver.trie.StreamSQLOrderedSource(ctx, name, key, field, desc, nullsFirst, nullsLast, visit)
}

func BenchmarkHatTrieSQLOrderedRangeBaseline(b *testing.B) {
	trie := ch002BenchmarkHatTrie(b)
	resolver := ch002HatTrieLegacyResolver{trie: trie}
	query := ch002HatTrieRangeQuery()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		ch002HatTrieBenchmarkSink = result
	}
}

func BenchmarkHatTrieSQLOrderedRangeSparseMarks(b *testing.B) {
	trie := ch002BenchmarkHatTrie(b)
	query := ch002HatTrieRangeQuery()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryParameters(context.Background(), query, trie, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		ch002HatTrieBenchmarkSink = result
	}
}

func BenchmarkHatTrieSQLOrderedRangeStreamBaseline(b *testing.B) {
	trie := ch002BenchmarkHatTrie(b)
	resolver := ch002HatTrieLegacyResolver{trie: trie}
	query := ch002HatTrieRangeQuery()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		count := 0
		err := ExecuteSQLQueryRows(context.Background(), query, resolver, nil, SQLQueryOptions{}, func(_ []string, _ SQLRow) error {
			count++
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
		ch002HatTrieStreamBenchmarkSink = count
	}
}

func BenchmarkHatTrieSQLOrderedRangeStreamSparseMarks(b *testing.B) {
	trie := ch002BenchmarkHatTrie(b)
	query := ch002HatTrieRangeQuery()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		count := 0
		err := ExecuteSQLQueryRows(context.Background(), query, trie, nil, SQLQueryOptions{}, func(_ []string, _ SQLRow) error {
			count++
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
		ch002HatTrieStreamBenchmarkSink = count
	}
}

var ch002HatTrieBenchmarkSink SQLQueryResult
var ch002HatTrieStreamBenchmarkSink int

func ch002BenchmarkHatTrie(b *testing.B) *HatTrie {
	b.Helper()
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	trie.UpsertString("events", benchmarkKeysetJSONRows(100_000))
	if err := trie.CreateSQLJSONFieldIndex("events", "score"); err != nil {
		b.Fatal(err)
	}
	return trie
}

func ch002HatTrieRangeQuery() string {
	return "FROM CACHE('events') AS event WHERE event.score >= 90000 ORDER BY event.score LIMIT 10 SELECT event.id, event.score"
}
