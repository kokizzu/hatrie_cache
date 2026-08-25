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

func TestPreparedQueryValidatesTypesAndReusesImmutablePlan(t *testing.T) {
	cache := hatSql.NewPreparedQueryCache(2)
	prepared, err := hatSql.PrepareQuery(
		"SELECT name, $2 AS prepared_at FROM CACHE('users') WHERE score >= $1",
		[]hatSql.ParameterSpec{{Type: hatSql.ParameterInteger}, {Type: hatSql.ParameterTimestamp}},
		cache,
	)
	if err != nil {
		t.Fatalf("PrepareQuery() error = %v", err)
	}
	resolverCalls := 0
	resolver := hatSql.SourceResolverFunc(func(name, key string) ([]hatSql.Row, error) {
		resolverCalls++
		return []hatSql.Row{{"name": "Ada", "score": int64(7)}}, nil
	})

	if _, err := prepared.Execute(context.Background(), resolver, []interface{}{7.5, "2026-08-01T00:00:00Z"}, hatSql.QueryOptions{}); err == nil {
		t.Fatal("Execute() accepted a fractional INTEGER parameter")
	}
	if resolverCalls != 0 {
		t.Fatalf("resolver calls after invalid parameters = %d, want 0", resolverCalls)
	}
	result, err := prepared.Execute(context.Background(), resolver, []interface{}{int(7), "2026-08-01T00:00:00Z"}, hatSql.QueryOptions{})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["name"] != "Ada" {
		t.Fatalf("Execute() rows = %#v", result.Rows)
	}
	if _, err := prepared.Execute(context.Background(), resolver, []interface{}{int(7)}, hatSql.QueryOptions{}); err == nil {
		t.Fatal("Execute() accepted a missing declared parameter")
	}
	stats := cache.Stats()
	if stats.Entries != 1 || stats.Hits == 0 || stats.Misses != 1 {
		t.Fatalf("cache stats = %#v, want one immutable reused plan", stats)
	}
}

func TestPrepareQueryRejectsInvalidSchema(t *testing.T) {
	if _, err := hatSql.PrepareQuery("SELECT * FROM CACHE('users') WHERE name = $1", nil, nil); err == nil {
		t.Fatal("PrepareQuery() accepted an undeclared parameter")
	}
	if _, err := hatSql.PrepareQuery("SELECT * FROM CACHE('users') WHERE name = $1", []hatSql.ParameterSpec{{Type: "INVALID"}}, nil); err == nil {
		t.Fatal("PrepareQuery() accepted an unsupported parameter type")
	}
}
