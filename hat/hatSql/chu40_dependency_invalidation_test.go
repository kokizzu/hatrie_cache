package hatSql

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
)

func TestCHU40ResultCacheInvalidatesOnlyDependentEntries(t *testing.T) {
	cache := NewResultCacheWithDependencies(4)
	ctx := context.Background()
	events := 0
	users := 0

	if _, err := cache.ExecuteWithDependencies(ctx, "events-query", []ResultCacheDependency{{Kind: "CACHE", Key: "events"}}, func(context.Context) (QueryResult, error) {
		events++
		return QueryResult{Rows: []Row{{"id": int64(events)}}}, nil
	}); err != nil {
		t.Fatalf("events insert error = %v", err)
	}
	if _, err := cache.ExecuteWithDependencies(ctx, "users-query", []ResultCacheDependency{{Kind: "CACHE", Key: "users"}}, func(context.Context) (QueryResult, error) {
		users++
		return QueryResult{Rows: []Row{{"id": int64(users)}}}, nil
	}); err != nil {
		t.Fatalf("users insert error = %v", err)
	}

	if removed := cache.InvalidateDependency("CACHE", "events"); removed != 1 {
		t.Fatalf("removed entries = %d, want 1", removed)
	}
	if stats := cache.Stats(); stats.Entries != 1 {
		t.Fatalf("entries after selective invalidation = %d, want 1", stats.Entries)
	}

	if _, err := cache.ExecuteWithDependencies(ctx, "events-query", []ResultCacheDependency{{Kind: "CACHE", Key: "events"}}, func(context.Context) (QueryResult, error) {
		events++
		return QueryResult{Rows: []Row{{"id": int64(events)}}}, nil
	}); err != nil {
		t.Fatalf("events refill error = %v", err)
	}
	if _, err := cache.ExecuteWithDependencies(ctx, "users-query", []ResultCacheDependency{{Kind: "CACHE", Key: "users"}}, func(context.Context) (QueryResult, error) {
		users++
		return QueryResult{Rows: []Row{{"id": int64(users)}}}, nil
	}); err != nil {
		t.Fatalf("users hit error = %v", err)
	}
	if events != 2 || users != 1 {
		t.Fatalf("execution counts after selective invalidation = events:%d users:%d, want 2/1", events, users)
	}
}

func TestCHU40ResultCacheExplicitExecutionUsesInvalidationWithoutVersionResolver(t *testing.T) {
	cache := NewResultCacheWithDependencies(2)
	ctx := context.Background()
	calls := 0
	execute := func(context.Context) (QueryResult, error) {
		calls++
		return QueryResult{Rows: []Row{{"id": int64(calls)}}}, nil
	}
	dependency := []ResultCacheDependency{{Kind: "CACHE", Key: "events"}}

	first, err := cache.ExecuteWithDependencies(ctx, "events-query", dependency, execute)
	if err != nil {
		t.Fatalf("first explicit execution error = %v", err)
	}
	second, err := cache.ExecuteWithDependencies(ctx, "events-query", dependency, execute)
	if err != nil {
		t.Fatalf("explicit cache hit error = %v", err)
	}
	if calls != 1 || first.Rows[0]["id"] != int64(1) || second.Rows[0]["id"] != int64(1) {
		t.Fatalf("explicit cached result/calls = %#v/%#v/%d, want ids 1/1 and one call", first, second, calls)
	}

	if removed := cache.InvalidateDependencies([]ResultCacheDependency{dependency[0], dependency[0]}); removed != 1 {
		t.Fatalf("deduplicated invalidation removed = %d, want 1", removed)
	}
	if _, err := cache.ExecuteWithDependencies(ctx, "events-query", dependency, execute); err != nil {
		t.Fatalf("explicit refill error = %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls after explicit invalidation = %d, want 2", calls)
	}
}

func TestCHU40ResultCacheDefaultDoesNotEnableExplicitDependencyCaching(t *testing.T) {
	cache := NewResultCache(2)
	calls := 0
	execute := func(context.Context) (QueryResult, error) {
		calls++
		return QueryResult{Rows: []Row{{"id": int64(calls)}}}, nil
	}

	for range 2 {
		if _, err := cache.ExecuteWithDependencies(context.Background(), "events-query", []ResultCacheDependency{{Kind: "CACHE", Key: "events"}}, execute); err != nil {
			t.Fatalf("default explicit execution error = %v", err)
		}
	}
	if calls != 2 {
		t.Fatalf("default explicit execution calls = %d, want 2", calls)
	}
}

func TestCHU40VersionedResultCacheRecordsDependenciesForInvalidation(t *testing.T) {
	cache := NewResultCacheWithDependencies(2)
	calls := 0
	version := "v1"
	dependency := []ResultCacheDependency{{Kind: "CACHE", Key: "events"}}
	execute := func(context.Context) (QueryResult, error) {
		calls++
		return QueryResult{Rows: []Row{{"id": int64(calls)}}}, nil
	}
	versionFn := func() (string, bool) { return version, true }

	if _, err := cache.ExecuteVersionedWithDependencies(context.Background(), "events-query", versionFn, dependency, execute); err != nil {
		t.Fatalf("versioned insert error = %v", err)
	}
	if _, err := cache.ExecuteVersionedWithDependencies(context.Background(), "events-query", versionFn, dependency, execute); err != nil {
		t.Fatalf("versioned hit error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("versioned calls before invalidation = %d, want 1", calls)
	}
	if removed := cache.InvalidateDependency("CACHE", "events"); removed != 1 {
		t.Fatalf("versioned invalidation removed = %d, want 1", removed)
	}
	if _, err := cache.ExecuteVersionedWithDependencies(context.Background(), "events-query", versionFn, dependency, execute); err != nil {
		t.Fatalf("versioned refill error = %v", err)
	}
	if calls != 2 {
		t.Fatalf("versioned calls after invalidation = %d, want 2", calls)
	}
}

func TestCHU40SQLResultCacheExplicitInvalidationSkipsVersionResolver(t *testing.T) {
	sourceCalls := 0
	resolver := SourceResolverFunc(func(name, key string) ([]Row, error) {
		if name != "CACHE" || key != "events" {
			return nil, nil
		}
		sourceCalls++
		return []Row{{"id": int64(sourceCalls)}}, nil
	})
	cache := NewSQLResultCacheWithDependencies(2)
	options := SQLQueryOptions{ResultCache: cache, ResultCacheExplicitInvalidation: true}
	query := "SELECT id FROM CACHE('events')"

	first, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, options)
	if err != nil {
		t.Fatalf("first explicit SQL query error = %v", err)
	}
	second, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, options)
	if err != nil {
		t.Fatalf("explicit SQL cache hit error = %v", err)
	}
	if sourceCalls != 1 || first.Rows[0]["id"] != int64(1) || second.Rows[0]["id"] != int64(1) {
		t.Fatalf("explicit SQL source calls/rows = %d/%#v/%#v, want 1 and id 1", sourceCalls, first.Rows, second.Rows)
	}

	if removed := cache.InvalidateDependency("CACHE", "events"); removed != 1 {
		t.Fatalf("explicit SQL invalidation removed = %d, want 1", removed)
	}
	third, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, options)
	if err != nil {
		t.Fatalf("explicit SQL refill error = %v", err)
	}
	if sourceCalls != 2 || third.Rows[0]["id"] != int64(2) {
		t.Fatalf("explicit SQL source calls/rows after invalidation = %d/%#v, want 2 and id 2", sourceCalls, third.Rows)
	}
}

func TestCHU40SQLResultCacheVersionedPathRecordsDependencies(t *testing.T) {
	resolver := &sqlResultCacheVersionedResolver{
		rows:    []Row{{"id": int64(1)}},
		version: "v1",
	}
	cache := NewSQLResultCacheWithDependencies(2)
	options := SQLQueryOptions{ResultCache: cache}
	query := "SELECT id FROM CACHE('events')"

	if _, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, options); err != nil {
		t.Fatalf("first dependency-aware SQL query error = %v", err)
	}
	if _, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, options); err != nil {
		t.Fatalf("dependency-aware SQL cache hit error = %v", err)
	}
	if resolver.sourceCalls != 1 {
		t.Fatalf("dependency-aware SQL source calls before invalidation = %d, want 1", resolver.sourceCalls)
	}
	if removed := cache.InvalidateDependency("CACHE", "events"); removed != 1 {
		t.Fatalf("dependency-aware SQL invalidation removed = %d, want 1", removed)
	}
	if _, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, options); err != nil {
		t.Fatalf("dependency-aware SQL refill error = %v", err)
	}
	if resolver.sourceCalls != 2 {
		t.Fatalf("dependency-aware SQL source calls after invalidation = %d, want 2", resolver.sourceCalls)
	}
}

func TestCHU40DependencyIndexSurvivesResultCachePersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "result-cache.bin")
	cache := NewResultCacheWithDependencies(2)
	dependencies := []ResultCacheDependency{{Kind: "CACHE", Key: "events"}}
	version := func() (string, bool) { return "v1", true }
	execute := func(context.Context) (QueryResult, error) {
		return QueryResult{Rows: []Row{{"id": int64(1)}}}, nil
	}

	if _, err := cache.ExecuteVersionedWithDependencies(context.Background(), "events-query", version, dependencies, execute); err != nil {
		t.Fatalf("dependency-aware persistence insert error = %v", err)
	}
	if err := cache.Persist(path); err != nil {
		t.Fatalf("dependency-aware persistence write error = %v", err)
	}

	restored := NewResultCacheWithDependencies(2)
	if err := restored.Restore(path); err != nil {
		t.Fatalf("dependency-aware persistence restore error = %v", err)
	}
	if removed := restored.InvalidateDependency("CACHE", "events"); removed != 1 {
		t.Fatalf("restored dependency invalidation removed = %d, want 1", removed)
	}
	if stats := restored.Stats(); stats.Entries != 0 {
		t.Fatalf("restored entries after dependency invalidation = %d, want 0", stats.Entries)
	}
}

func TestCHU40DependencyIndexRemovesEvictedAndOverwrittenEntries(t *testing.T) {
	cache := NewResultCacheWithDependencies(1)
	execute := func(context.Context) (QueryResult, error) {
		return QueryResult{Rows: []Row{{"id": int64(1)}}}, nil
	}
	if _, err := cache.ExecuteWithDependencies(context.Background(), "events-query", []ResultCacheDependency{{Kind: "CACHE", Key: "events"}}, execute); err != nil {
		t.Fatalf("events insert error = %v", err)
	}
	if _, err := cache.ExecuteWithDependencies(context.Background(), "users-query", []ResultCacheDependency{{Kind: "CACHE", Key: "users"}}, execute); err != nil {
		t.Fatalf("users insert error = %v", err)
	}
	if removed := cache.InvalidateDependency("CACHE", "events"); removed != 0 {
		t.Fatalf("evicted events invalidation removed = %d, want 0", removed)
	}
	if removed := cache.InvalidateDependency("CACHE", "users"); removed != 1 {
		t.Fatalf("users invalidation after eviction removed = %d, want 1", removed)
	}

	if _, err := cache.ExecuteWithDependencies(context.Background(), "shared-query", []ResultCacheDependency{{Kind: "CACHE", Key: "events"}}, execute); err != nil {
		t.Fatalf("shared events insert error = %v", err)
	}
	if _, err := cache.ExecuteWithDependencies(context.Background(), "shared-query", []ResultCacheDependency{{Kind: "CACHE", Key: "users"}}, execute); err != nil {
		t.Fatalf("shared users overwrite error = %v", err)
	}
	if removed := cache.InvalidateDependency("CACHE", "events"); removed != 0 {
		t.Fatalf("overwritten events invalidation removed = %d, want 0", removed)
	}
	if removed := cache.InvalidateDependency("CACHE", "users"); removed != 1 {
		t.Fatalf("overwritten users invalidation removed = %d, want 1", removed)
	}
}

func TestCHU40DependencyInvalidationDoesNotRetainInFlightStaleResults(t *testing.T) {
	cache := NewResultCacheWithDependencies(1)
	dependency := []ResultCacheDependency{{Kind: "CACHE", Key: "events"}}
	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	var once sync.Once
	calls := 0
	go func() {
		defer close(finished)
		_, _ = cache.ExecuteWithDependencies(context.Background(), "events-query", dependency, func(context.Context) (QueryResult, error) {
			calls++
			once.Do(func() { close(started) })
			<-release
			return QueryResult{Rows: []Row{{"id": int64(calls)}}}, nil
		})
	}()
	<-started
	if removed := cache.InvalidateDependency("CACHE", "events"); removed != 0 {
		t.Fatalf("in-flight invalidation removed = %d, want 0", removed)
	}
	close(release)
	<-finished

	if _, err := cache.ExecuteWithDependencies(context.Background(), "events-query", dependency, func(context.Context) (QueryResult, error) {
		calls++
		return QueryResult{Rows: []Row{{"id": int64(calls)}}}, nil
	}); err != nil {
		t.Fatalf("post-invalidation execution error = %v", err)
	}
	if calls != 2 {
		t.Fatalf("in-flight stale result calls = %d, want 2", calls)
	}
}
