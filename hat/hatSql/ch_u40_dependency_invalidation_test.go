package hatSql

import (
	"context"
	"testing"
)

func TestCHU40ResultCacheDependencyInvalidationIsSelective(t *testing.T) {
	cache := NewResultCacheWithDependencies(4)
	var ordersCalls, usersCalls int
	orders := []ResultCacheDependency{{Kind: "table", Key: "orders"}}
	users := []ResultCacheDependency{{Kind: "table", Key: "users"}}
	query := func(calls *int, value int64) func(context.Context) (QueryResult, error) {
		return func(context.Context) (QueryResult, error) {
			*calls = *calls + 1
			return QueryResult{Rows: []Row{{"value": value}}}, nil
		}
	}

	if _, err := cache.ExecuteWithDependencies(context.Background(), "orders-total", orders, query(&ordersCalls, 1)); err != nil {
		t.Fatalf("orders seed error = %v", err)
	}
	if _, err := cache.ExecuteWithDependencies(context.Background(), "users-total", users, query(&usersCalls, 2)); err != nil {
		t.Fatalf("users seed error = %v", err)
	}
	if _, err := cache.ExecuteWithDependencies(context.Background(), "orders-total", orders, query(&ordersCalls, 3)); err != nil {
		t.Fatalf("orders hit error = %v", err)
	}
	if ordersCalls != 1 || usersCalls != 1 {
		t.Fatalf("seed/hit calls = orders:%d users:%d, want 1/1", ordersCalls, usersCalls)
	}
	if invalidated := cache.InvalidateDependencies(orders); invalidated != 1 {
		t.Fatalf("InvalidateDependencies() = %d, want 1", invalidated)
	}
	if _, err := cache.ExecuteWithDependencies(context.Background(), "orders-total", orders, query(&ordersCalls, 4)); err != nil {
		t.Fatalf("orders post-invalidation error = %v", err)
	}
	if stats := cache.Stats(); stats.Entries != 2 {
		t.Fatalf("Entries after re-seed = %d, want 2", stats.Entries)
	}
	if _, err := cache.ExecuteWithDependencies(context.Background(), "users-total", users, query(&usersCalls, 5)); err != nil {
		t.Fatalf("users unaffected error = %v", err)
	}
	if ordersCalls != 2 || usersCalls != 1 {
		t.Fatalf("post-invalidation calls = orders:%d users:%d, want 2/1", ordersCalls, usersCalls)
	}
	if stats := cache.Stats(); stats.Hits != 2 {
		t.Fatalf("Hits = %d, want 2", stats.Hits)
	}
}

func TestCHU40ResultCacheDependencyOrderAndDuplicateValidation(t *testing.T) {
	cache := NewResultCacheWithDependencies(2)
	dependencies := []ResultCacheDependency{{Kind: "table", Key: "users"}, {Kind: "table", Key: "orders"}}
	var calls int
	run := func(context.Context) (QueryResult, error) {
		calls++
		return QueryResult{Rows: []Row{{"value": int64(calls)}}}, nil
	}
	if _, err := cache.ExecuteWithDependencies(context.Background(), "dashboard", dependencies, run); err != nil {
		t.Fatalf("first ExecuteWithDependencies() error = %v", err)
	}
	if _, err := cache.ExecuteWithDependencies(context.Background(), "dashboard", []ResultCacheDependency{{Kind: "table", Key: "orders"}, {Kind: "table", Key: "users"}}, run); err != nil {
		t.Fatalf("reordered ExecuteWithDependencies() error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("reordered dependency calls = %d, want 1", calls)
	}
	if _, err := cache.ExecuteWithDependencies(context.Background(), "dashboard", []ResultCacheDependency{{Kind: "table", Key: "users"}, {Kind: "table", Key: "orders"}, {Kind: "table", Key: "users"}}, run); err != nil {
		t.Fatalf("duplicate dependency execution error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("duplicate dependency calls = %d, want 1", calls)
	}
}

func TestCHU40ResultCacheInFlightInvalidationDoesNotRetainResult(t *testing.T) {
	cache := NewResultCacheWithDependencies(2)
	dependencies := []ResultCacheDependency{{Kind: "table", Key: "orders"}}
	started := make(chan struct{})
	release := make(chan struct{})
	resultCh := make(chan error, 1)
	go func() {
		_, err := cache.ExecuteWithDependencies(context.Background(), "orders-total", dependencies, func(context.Context) (QueryResult, error) {
			close(started)
			<-release
			return QueryResult{Rows: []Row{{"value": int64(1)}}}, nil
		})
		resultCh <- err
	}()
	<-started
	if invalidated := cache.InvalidateDependencies(dependencies); invalidated != 0 {
		t.Fatalf("in-flight InvalidateDependencies() = %d, want 0", invalidated)
	}
	close(release)
	if err := <-resultCh; err != nil {
		t.Fatalf("in-flight execution error = %v", err)
	}
	calls := 0
	if _, err := cache.ExecuteWithDependencies(context.Background(), "orders-total", dependencies, func(context.Context) (QueryResult, error) {
		calls++
		return QueryResult{Rows: []Row{{"value": int64(2)}}}, nil
	}); err != nil {
		t.Fatalf("post-invalidation execution error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("post-invalidation executions = %d, want 1", calls)
	}
}
