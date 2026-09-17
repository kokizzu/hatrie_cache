package hatSql

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

const mz045CanonicalPlanQuery = "FROM VALUES (1), (2), (3) AS values(id) SELECT id WHERE id >= 2"

func mz045EquivalentPlanQueries() []string {
	queries := make([]string, 64)
	for index := range queries {
		spaces := strings.Repeat(" ", index+1)
		tabs := strings.Repeat("\t", index%5+1)
		queries[index] = "FROM" + spaces + "VALUES (1), (2), (3) AS values(id)" + tabs + "SELECT id WHERE id >= 2"
	}
	return queries
}

func TestMZ045CompiledPlanCacheReusesEquivalentTokenStreams(t *testing.T) {
	cache, err := NewSQLCompiledQueryCache(SQLCompiledQueryCacheOptions{MaxEntries: 8, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	first, err := cache.Compile(mz045CanonicalPlanQuery)
	if err != nil {
		t.Fatal(err)
	}
	second, err := cache.Compile("from\tvalues (1), (2), (3) as values(id)\nselect id where id >= 2")
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("equivalent token streams returned different compiled handles")
	}
	result, err := second.Execute(context.Background(), nil, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []SQLRow{{"id": int64(2)}, {"id": int64(3)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("equivalent compiled rows = %#v, want %#v", result.Rows, want)
	}
	stats := cache.Stats()
	if stats.Entries != 1 || stats.Misses != 1 || stats.Hits != 1 {
		t.Fatalf("canonical cache stats = %#v, want one entry and one hit/miss", stats)
	}
}

func TestMZ045CompiledPlanCacheKeepsLiteralValuesDistinct(t *testing.T) {
	cache, err := NewSQLCompiledQueryCache(SQLCompiledQueryCacheOptions{MaxEntries: 8, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	first, err := cache.Compile("FROM VALUES (1), (2), (3) AS values(id) SELECT id WHERE id >= 2")
	if err != nil {
		t.Fatal(err)
	}
	second, err := cache.Compile("FROM VALUES (1), (2), (3) AS values(id) SELECT id WHERE id >= 3")
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("different literal values shared a compiled handle")
	}
	firstResult, err := first.Execute(context.Background(), nil, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	secondResult, err := second.Execute(context.Background(), nil, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []SQLRow{{"id": int64(2)}, {"id": int64(3)}}; !reflect.DeepEqual(firstResult.Rows, want) {
		t.Fatalf("first literal rows = %#v, want %#v", firstResult.Rows, want)
	}
	if want := []SQLRow{{"id": int64(3)}}; !reflect.DeepEqual(secondResult.Rows, want) {
		t.Fatalf("second literal rows = %#v, want %#v", secondResult.Rows, want)
	}
}

func TestMZ045CompiledPlanCacheEvictionDropsCanonicalEntry(t *testing.T) {
	cache, err := NewSQLCompiledQueryCache(SQLCompiledQueryCacheOptions{MaxEntries: 1, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	first, err := cache.Compile(mz045CanonicalPlanQuery)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Compile("FROM VALUES (9) AS values(id) SELECT id"); err != nil {
		t.Fatal(err)
	}
	reloaded, err := cache.Compile("from values(1),(2),(3) as values(id) select id where id >= 2")
	if err != nil {
		t.Fatal(err)
	}
	if first == reloaded {
		t.Fatal("evicted canonical plan was returned after reload")
	}
	if stats := cache.Stats(); stats.Entries != 1 || stats.Evictions != 2 {
		t.Fatalf("eviction stats = %#v, want one entry and two evictions", stats)
	}
}

var mz045PlanEquivalenceBenchmarkSink *CompiledSQLQuery

func BenchmarkMZ045EquivalentPlanCompileBaseline(b *testing.B) {
	queries := mz045EquivalentPlanQueries()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for _, source := range queries {
			query, err := CompileSQLQuery(source)
			if err != nil {
				b.Fatal(err)
			}
			mz045PlanEquivalenceBenchmarkSink = query
		}
	}
}

func BenchmarkMZ045EquivalentPlanCache(b *testing.B) {
	queries := mz045EquivalentPlanQueries()
	cache, err := NewSQLCompiledQueryCache(SQLCompiledQueryCacheOptions{MaxEntries: 128, MaxBytes: 1 << 20})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for _, source := range queries {
			query, err := cache.Compile(source)
			if err != nil {
				b.Fatal(err)
			}
			mz045PlanEquivalenceBenchmarkSink = query
		}
		b.StopTimer()
		cache.Invalidate()
		b.StartTimer()
	}
}

func BenchmarkMZ045ExactPlanCacheHit(b *testing.B) {
	cache, err := NewSQLCompiledQueryCache(SQLCompiledQueryCacheOptions{MaxEntries: 8, MaxBytes: 1 << 20})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := cache.Compile(mz045CanonicalPlanQuery); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		query, err := cache.Compile(mz045CanonicalPlanQuery)
		if err != nil {
			b.Fatal(err)
		}
		mz045PlanEquivalenceBenchmarkSink = query
	}
}
