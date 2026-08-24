package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestExecuteSQLQueryWithExternalResolver(t *testing.T) {
	cache := hatSql.NewPreparedQueryCache(2)
	resolver := hatSql.SourceResolverFunc(func(name, key string) ([]hatSql.Row, error) {
		if name != "CACHE" || key != "users" {
			t.Fatalf("ResolveSQLSource(%q, %q)", name, key)
		}
		return []hatSql.Row{{"name": "Ada", "score": int64(7)}}, nil
	})

	result, err := hatSql.ExecuteQueryParameters(context.Background(), "SELECT name FROM CACHE('users') WHERE score >= $1", resolver, []interface{}{int64(7)}, hatSql.QueryOptions{PreparedCache: cache})
	if err != nil {
		t.Fatalf("ExecuteQueryParameters() error = %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["name"] != "Ada" {
		t.Fatalf("ExecuteQueryParameters() rows = %#v", result.Rows)
	}

	if _, err := hatSql.ParseQueryWithCache("SELECT name FROM CACHE('users') WHERE score >= $1", nil, cache); err == nil {
		t.Fatal("ParseQueryWithCache() without $1 should fail")
	}
	stats := cache.Stats()
	if stats.Hits == 0 || stats.Entries != 1 {
		t.Fatalf("cache stats = %#v, want a reused template", stats)
	}
}
