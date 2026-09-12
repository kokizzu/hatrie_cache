package hatSql

import (
	"context"
	"errors"
	"testing"
)

func TestResultCacheStatsCountHitsMissesBypassesAndEvictions(t *testing.T) {
	cache := NewSQLResultCache(1)
	version := "v1"
	executions := 0
	execute := func(context.Context) (QueryResult, error) {
		executions++
		return QueryResult{Rows: []Row{{"value": executions}}}, nil
	}
	versionFn := func() (string, bool) { return version, true }

	if _, err := cache.ExecuteVersioned(context.Background(), "first", versionFn, execute); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.ExecuteVersioned(context.Background(), "first", versionFn, execute); err != nil {
		t.Fatal(err)
	}
	version = "v2"
	if _, err := cache.ExecuteVersioned(context.Background(), "first", versionFn, execute); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.ExecuteVersioned(context.Background(), "second", versionFn, execute); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.ExecuteVersioned(context.Background(), "unavailable", func() (string, bool) { return "", false }, execute); err != nil {
		t.Fatal(err)
	}
	cache.RecordBypass()

	stats := cache.Stats()
	if stats.Entries != 1 || stats.Hits != 1 || stats.Misses != 3 || stats.Bypasses != 2 || stats.Evictions != 1 {
		t.Fatalf("cache.Stats() = %#v, want entries=1 hits=1 misses=3 bypasses=2 evictions=1", stats)
	}
	if executions != 4 {
		t.Fatalf("executions = %d, want 4", executions)
	}
}

func TestResultCacheStatsCountDisabledCacheBypass(t *testing.T) {
	cache := NewSQLResultCache(0)
	_, err := cache.ExecuteVersioned(context.Background(), "disabled", func() (string, bool) { return "v1", true }, func(context.Context) (QueryResult, error) {
		return QueryResult{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	stats := cache.Stats()
	if stats.Bypasses != 1 || stats.Hits != 0 || stats.Misses != 0 || stats.Evictions != 0 {
		t.Fatalf("disabled cache stats = %#v", stats)
	}
}

func TestResultCacheStatsCountLegacyEpochPath(t *testing.T) {
	cache := NewResultCache(1)
	epoch := uint64(1)
	execute := func(context.Context) (QueryResult, error) {
		return QueryResult{Rows: []Row{{"value": "ok"}}}, nil
	}
	version := func() uint64 { return epoch }

	if _, err := cache.Execute(context.Background(), "legacy", version, execute); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Execute(context.Background(), "legacy", version, execute); err != nil {
		t.Fatal(err)
	}
	epoch = 2
	if _, err := cache.Execute(context.Background(), "legacy", version, execute); err != nil {
		t.Fatal(err)
	}
	stats := cache.Stats()
	if stats.Hits != 1 || stats.Misses != 2 || stats.Bypasses != 0 || stats.Evictions != 0 {
		t.Fatalf("legacy cache stats = %#v", stats)
	}
}

func TestResultCacheStatsCountUnstableEpochAsBypass(t *testing.T) {
	cache := NewResultCache(1)
	epoch := uint64(1)
	_, err := cache.Execute(context.Background(), "unstable", func() uint64 { return epoch }, func(context.Context) (QueryResult, error) {
		epoch = 2
		return QueryResult{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	stats := cache.Stats()
	if stats.Hits != 0 || stats.Misses != 1 || stats.Bypasses != 1 || stats.Entries != 0 {
		t.Fatalf("unstable epoch stats = %#v", stats)
	}
}

func TestSQLQueryResultCacheStatsCountIneligibleQueryBypass(t *testing.T) {
	cache := NewSQLResultCache(1)
	resolver := &c208ResultCacheResolver{}
	_, err := ExecuteSQLQueryParameters(context.Background(), "SELECT id FROM CACHE('events')", resolver, nil, SQLQueryOptions{
		ResultCache: cache,
		MaxRows:     1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats := cache.Stats(); stats.Bypasses != 1 {
		t.Fatalf("ineligible query stats = %#v, want one bypass", stats)
	}
}

func TestResultCacheStatsNilCacheIsSafe(t *testing.T) {
	var cache *ResultCache
	if stats := cache.Stats(); stats != (ResultCacheStats{}) {
		t.Fatalf("nil cache stats = %#v", stats)
	}
	cache.RecordBypass()
}

type c208ResultCacheResolver struct{}

func (resolver *c208ResultCacheResolver) ResolveSQLSource(_ string, _ string) ([]Row, error) {
	return []Row{{"id": int64(1)}}, nil
}

func (resolver *c208ResultCacheResolver) SQLSourceVersion(_ string, _ string) (string, bool, error) {
	return "v1", true, nil
}

var _ SQLSourceResolver = (*c208ResultCacheResolver)(nil)
var _ SourceVersionResolver = (*c208ResultCacheResolver)(nil)

func TestResultCacheStatsExecutorErrorsDoNotBecomeHits(t *testing.T) {
	cache := NewSQLResultCache(1)
	wantErr := errors.New("executor failed")
	_, err := cache.ExecuteVersioned(context.Background(), "error", func() (string, bool) { return "v1", true }, func(context.Context) (QueryResult, error) {
		return QueryResult{}, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("ExecuteVersioned() error = %v, want %v", err, wantErr)
	}
	if stats := cache.Stats(); stats.Hits != 0 || stats.Misses != 1 || stats.Entries != 0 {
		t.Fatalf("executor error stats = %#v", stats)
	}
}
