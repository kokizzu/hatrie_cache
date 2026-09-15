package hatCache

import (
	"context"
	"encoding/json"
	"testing"

	"hatrie_cache/hat/hatSql"
)

var benchmarkCHU08Result SQLQueryResult

func newCHU08AggregateTrie(b *testing.B) *HatTrie {
	b.Helper()
	rows := make([]map[string]interface{}, 20_000)
	for index := range rows {
		rows[index] = map[string]interface{}{
			"id":    index,
			"team":  index % 20,
			"value": index * 3,
		}
	}
	data, err := json.Marshal(rows)
	if err != nil {
		b.Fatal(err)
	}
	trie := CreateHatTrie()
	if err := trie.UpsertStringChecked("people", string(data)); err != nil {
		trie.Destroy()
		b.Fatal(err)
	}
	return trie
}

func BenchmarkCHU08SQLQueryUncachedDirect(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	if err := trie.UpsertStringChecked("people", `[{"id":1,"name":"Ada"}]`); err != nil {
		b.Fatal(err)
	}
	query := "FROM CACHE('people') AS person SELECT person.id, person.name"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := hatSql.ExecuteSQLQueryParameters(context.Background(), query, trie, nil, hatSql.SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		benchmarkCHU08Result = result
	}
}

func BenchmarkCHU08SQLQueryAutomaticDisabled(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	if err := trie.UpsertStringChecked("people", `[{"id":1,"name":"Ada"}]`); err != nil {
		b.Fatal(err)
	}
	query := "FROM CACHE('people') AS person SELECT person.id, person.name"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := ExecuteSQLQueryParameters(context.Background(), query, trie, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		benchmarkCHU08Result = result
	}
}

func BenchmarkCHU08SQLQueryAutomaticCacheHit(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	if err := trie.UpsertStringChecked("people", `[{"id":1,"name":"Ada"}]`); err != nil {
		b.Fatal(err)
	}
	if err := trie.ConfigureSQLResultCache(2); err != nil {
		b.Fatal(err)
	}
	query := "FROM CACHE('people') AS person SELECT person.id, person.name"
	if _, err := ExecuteSQLQueryParameters(context.Background(), query, trie, nil, SQLQueryOptions{}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := ExecuteSQLQueryParameters(context.Background(), query, trie, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		benchmarkCHU08Result = result
	}
}

func BenchmarkCHU08SQLAggregateUncachedDirect(b *testing.B) {
	trie := newCHU08AggregateTrie(b)
	defer trie.Destroy()
	query := "FROM CACHE('people') AS person GROUP BY person.team SELECT person.team, count() AS total"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := hatSql.ExecuteSQLQueryParameters(context.Background(), query, trie, nil, hatSql.SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		benchmarkCHU08Result = result
	}
}

func BenchmarkCHU08SQLAggregateAutomaticCacheHit(b *testing.B) {
	trie := newCHU08AggregateTrie(b)
	defer trie.Destroy()
	if err := trie.ConfigureSQLResultCache(2); err != nil {
		b.Fatal(err)
	}
	query := "FROM CACHE('people') AS person GROUP BY person.team SELECT person.team, count() AS total"
	if _, err := ExecuteSQLQueryParameters(context.Background(), query, trie, nil, SQLQueryOptions{}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := ExecuteSQLQueryParameters(context.Background(), query, trie, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		benchmarkCHU08Result = result
	}
}
