package hatSql

import (
	"context"
	"testing"
)

type c206VolatileResolver struct {
	*sqlResultCacheVersionedResolver
	functionCalls int
}

func (resolver *c206VolatileResolver) EvaluateSQLFunction(name string, calls []FunctionCall) ([]interface{}, error) {
	values := make([]interface{}, len(calls))
	for index := range calls {
		resolver.functionCalls++
		values[index] = int64(resolver.functionCalls)
	}
	return values, nil
}

func TestSQLResultCacheBypassesVolatileTopLevelQueries(t *testing.T) {
	resolver := &c206VolatileResolver{sqlResultCacheVersionedResolver: &sqlResultCacheVersionedResolver{
		rows:    []Row{{"id": int64(1)}},
		version: "v1",
	}}
	cache := NewSQLResultCache(2)
	query := "FROM CACHE('events') SELECT id, VOLATILE() AS observed_at"
	options := SQLQueryOptions{ResultCache: cache}
	for range 2 {
		if _, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, options); err != nil {
			t.Fatalf("volatile query error = %v", err)
		}
	}
	if resolver.sourceCalls != 2 || resolver.functionCalls != 2 {
		t.Fatalf("volatile top-level calls = source %d/function %d, want 2/2", resolver.sourceCalls, resolver.functionCalls)
	}
	if stats := cache.Stats(); stats.Hits != 0 || stats.Misses != 0 {
		t.Fatalf("volatile top-level cache stats = %#v, want no cache activity", stats)
	}
}

func TestSQLResultCacheMarksVolatileTopLevelQueries(t *testing.T) {
	query, err := parseSQLQuery("FROM CACHE('events') SELECT id, NOW() AS observed_at")
	if err != nil {
		t.Fatalf("parse volatile query = %v", err)
	}
	if !query.cacheVolatile {
		t.Fatal("volatile top-level query was not marked volatile")
	}
	if _, ok := sqlResultCacheQueryKey(query, nil, SQLQueryOptions{}); ok {
		t.Fatal("volatile top-level query received a result-cache key")
	}
}

func TestSQLResultCacheCachesDeterministicTopLevelQueries(t *testing.T) {
	resolver := &sqlResultCacheVersionedResolver{
		rows:    []Row{{"id": int64(1)}},
		version: "v1",
	}
	cache := NewSQLResultCache(2)
	query := "FROM CACHE('events') SELECT id"
	options := SQLQueryOptions{ResultCache: cache}
	for range 2 {
		if _, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, options); err != nil {
			t.Fatalf("deterministic query error = %v", err)
		}
	}
	if resolver.sourceCalls != 1 {
		t.Fatalf("deterministic top-level source calls = %d, want 1", resolver.sourceCalls)
	}
	if stats := cache.Stats(); stats.Hits != 1 || stats.Misses != 1 {
		t.Fatalf("deterministic top-level cache stats = %#v, want one miss and one hit", stats)
	}
}

func TestSQLResultCacheKeyRejectsVolatileSources(t *testing.T) {
	for _, source := range []string{
		"FROM CACHE('events') SELECT CURRENT_DATE",
		"FROM CACHE('events') SELECT CURRENT_TIME",
		"FROM CACHE('events') SELECT CURRENT_TIMESTAMP",
		"FROM CACHE('events') SELECT NOW()",
		"FROM CACHE('events') SELECT RAND()",
		"FROM CACHE('events') SELECT RANDOM()",
		"FROM CACHE('events') SELECT UUID()",
		"FROM CACHE('events') SELECT GENERATE_UUID()",
	} {
		if _, ok := sqlResultCacheKey(source, nil, SQLQueryOptions{}); ok {
			t.Fatalf("sqlResultCacheKey(%q) accepted volatile source", source)
		}
	}
}
