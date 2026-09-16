package hatSql

import (
	"context"
	"testing"
)

func TestSQLSubqueryResultCacheReusesAndInvalidatesDerivedQueries(t *testing.T) {
	resolver := &sqlResultCacheVersionedResolver{
		rows:    []Row{{"id": int64(1)}, {"id": int64(2)}},
		version: "v1",
	}
	cache := NewSQLResultCache(2)
	options := SQLQueryOptions{SubqueryResultCache: cache}
	query := "FROM (FROM CACHE('events') SELECT id) AS inner_rows SELECT inner_rows.id ORDER BY inner_rows.id"

	first, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, options)
	if err != nil {
		t.Fatalf("first query error = %v", err)
	}
	second, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, options)
	if err != nil {
		t.Fatalf("cached query error = %v", err)
	}
	if resolver.sourceCalls != 1 {
		t.Fatalf("source calls after subquery cache hit = %d, want 1", resolver.sourceCalls)
	}
	if len(first.Rows) != 2 || len(second.Rows) != 2 || first.Rows[0]["id"] != int64(1) || second.Rows[1]["id"] != int64(2) {
		t.Fatalf("cached rows = %#v and %#v", first.Rows, second.Rows)
	}
	second.Rows[0]["id"] = int64(99)
	independent, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, options)
	if err != nil {
		t.Fatalf("independent cached query error = %v", err)
	}
	if independent.Rows[0]["id"] != int64(1) || resolver.sourceCalls != 1 {
		t.Fatalf("cached result mutation/source calls = %#v/%d, want id 1 and one source call", independent.Rows, resolver.sourceCalls)
	}
	if stats := cache.Stats(); stats.Hits != 2 || stats.Misses != 1 {
		t.Fatalf("subquery cache stats = %#v, want one miss and two hits", stats)
	}

	resolver.rows = []Row{{"id": int64(3)}}
	resolver.version = "v2"
	third, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, options)
	if err != nil {
		t.Fatalf("invalidated query error = %v", err)
	}
	if resolver.sourceCalls != 2 || len(third.Rows) != 1 || third.Rows[0]["id"] != int64(3) {
		t.Fatalf("invalidated source calls/rows = %d/%#v, want 2 and id 3", resolver.sourceCalls, third.Rows)
	}
}

func TestSQLSubqueryResultCacheIsOffByDefault(t *testing.T) {
	resolver := &sqlResultCacheVersionedResolver{
		rows:    []Row{{"id": int64(1)}},
		version: "v1",
	}
	query := "FROM (FROM CACHE('events') SELECT id) AS inner_rows SELECT inner_rows.id"
	for range 2 {
		if _, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{}); err != nil {
			t.Fatalf("default query error = %v", err)
		}
	}
	if resolver.sourceCalls != 2 {
		t.Fatalf("default source calls = %d, want 2", resolver.sourceCalls)
	}
}

func TestSQLSubqueryResultCacheCachesCTEAndUnionBranches(t *testing.T) {
	resolver := &sqlResultCacheVersionedResolver{
		rows:    []Row{{"id": int64(1)}, {"id": int64(2)}},
		version: "v1",
	}
	cache := NewSQLResultCache(8)
	options := SQLQueryOptions{SubqueryResultCache: cache}

	cteQuery := "WITH events AS (FROM CACHE('events') SELECT id) FROM events SELECT id"
	if _, err := ExecuteSQLQueryParameters(context.Background(), cteQuery, resolver, nil, options); err != nil {
		t.Fatalf("first CTE query error = %v", err)
	}
	if _, err := ExecuteSQLQueryParameters(context.Background(), cteQuery, resolver, nil, options); err != nil {
		t.Fatalf("cached CTE query error = %v", err)
	}
	if resolver.sourceCalls != 1 {
		t.Fatalf("CTE source calls = %d, want 1", resolver.sourceCalls)
	}

	unionQuery := "FROM CACHE('events') SELECT id WHERE id = 1 UNION ALL FROM CACHE('events') SELECT id WHERE id = 2"
	first, err := ExecuteSQLQueryParameters(context.Background(), unionQuery, resolver, nil, options)
	if err != nil {
		t.Fatalf("first UNION query error = %v", err)
	}
	second, err := ExecuteSQLQueryParameters(context.Background(), unionQuery, resolver, nil, options)
	if err != nil {
		t.Fatalf("cached UNION query error = %v", err)
	}
	if resolver.sourceCalls != 3 {
		t.Fatalf("UNION source calls = %d, want 3 after two branch misses", resolver.sourceCalls)
	}
	if len(first.Rows) != 2 || len(second.Rows) != 2 {
		t.Fatalf("UNION rows = %#v and %#v, want two rows each", first.Rows, second.Rows)
	}
}

func TestSQLSubqueryResultCacheBypassesVolatileDerivedQueries(t *testing.T) {
	resolver := &sqlResultCacheVersionedResolver{
		rows:    []Row{{"id": int64(1)}},
		version: "v1",
	}
	options := SQLQueryOptions{SubqueryResultCache: NewSQLResultCache(2)}
	query := "FROM (FROM CACHE('events') SELECT id, NOW() AS observed_at) AS inner_rows SELECT inner_rows.id"
	for range 2 {
		if _, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, options); err != nil {
			t.Fatalf("volatile query error = %v", err)
		}
	}
	if resolver.sourceCalls != 2 {
		t.Fatalf("volatile source calls = %d, want 2", resolver.sourceCalls)
	}
}

func TestSQLSubqueryResultCacheSeparatesBoundParameters(t *testing.T) {
	resolver := &sqlResultCacheVersionedResolver{
		rows:    []Row{{"id": int64(1)}, {"id": int64(2)}},
		version: "v1",
	}
	cache := NewSQLResultCache(4)
	options := SQLQueryOptions{SubqueryResultCache: cache}
	query := "FROM (FROM CACHE('events') SELECT id WHERE id = $1) AS inner_rows SELECT inner_rows.id"

	first, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, []interface{}{int64(1)}, options)
	if err != nil {
		t.Fatalf("first parameterized subquery error = %v", err)
	}
	second, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, []interface{}{int64(2)}, options)
	if err != nil {
		t.Fatalf("second parameterized subquery error = %v", err)
	}
	firstAgain, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, []interface{}{int64(1)}, options)
	if err != nil {
		t.Fatalf("cached parameterized subquery error = %v", err)
	}
	if resolver.sourceCalls != 2 {
		t.Fatalf("parameterized subquery source calls = %d, want 2", resolver.sourceCalls)
	}
	if len(first.Rows) != 1 || first.Rows[0]["id"] != int64(1) || len(second.Rows) != 1 || second.Rows[0]["id"] != int64(2) || len(firstAgain.Rows) != 1 || firstAgain.Rows[0]["id"] != int64(1) {
		t.Fatalf("parameterized subquery rows = %#v, %#v, %#v", first.Rows, second.Rows, firstAgain.Rows)
	}
}

func TestSQLSubqueryResultCacheDoesNotInterceptScalarSubqueries(t *testing.T) {
	resolver := &sqlResultCacheVersionedResolver{
		rows:    []Row{{"id": int64(1)}},
		version: "v1",
	}
	cache := NewSQLResultCache(2)
	options := SQLQueryOptions{SubqueryResultCache: cache}
	query := "FROM CACHE('events') SELECT (FROM CACHE('events') SELECT id LIMIT 1) AS nested_id"
	for range 2 {
		if _, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, options); err != nil {
			t.Fatalf("scalar subquery error = %v", err)
		}
	}
	if resolver.sourceCalls != 2 {
		t.Fatalf("scalar subquery source calls = %d, want 2 with per-execution source reuse", resolver.sourceCalls)
	}
	if stats := cache.Stats(); stats.Hits != 0 || stats.Misses != 0 {
		t.Fatalf("scalar subquery result-cache stats = %#v, want no subquery cache activity", stats)
	}
}
