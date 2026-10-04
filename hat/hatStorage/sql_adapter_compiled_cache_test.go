package hatStorage_test

import (
	"context"
	"errors"
	"testing"

	"hatrie_cache/hat/hatSql"
	"hatrie_cache/hat/hatStorage"
)

func TestSQLAdapterRegistryUsesBoundedCompiledCacheByDefault(t *testing.T) {
	registry, err := hatStorage.NewSQLAdapterRegistry(nil, hatStorage.SQLResolverAdapter{
		NamespaceName: "remote",
		Resolver: hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
			return nil, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := registry.Execute(context.Background(), "remote", "SELECT * FROM CACHE('items')", nil, hatSql.SQLQueryOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	stats := registry.CompiledQueryCacheStats()
	if stats.Misses != 1 || stats.Hits != 1 || stats.Entries != 1 {
		t.Fatalf("default compiled cache stats = %#v, want one miss, one hit, one entry", stats)
	}
}

func TestSQLAdapterRegistryAcceptsCallerCompiledCacheAndCanDisableIt(t *testing.T) {
	cache, err := hatSql.NewSQLCompiledQueryCache(hatSql.SQLCompiledQueryCacheOptions{MaxEntries: 2, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	adapter := hatStorage.SQLResolverAdapter{
		NamespaceName: "remote",
		Resolver: hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
			return nil, nil
		}),
	}
	registry, err := hatStorage.NewSQLAdapterRegistryWithOptions(hatStorage.SQLAdapterRegistryOptions{CompiledCache: cache}, adapter)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Execute(context.Background(), "remote", "SELECT * FROM CACHE('items')", nil, hatSql.SQLQueryOptions{}); err != nil {
		t.Fatal(err)
	}
	if got := cache.Stats(); got.Misses != 1 || got.Entries != 1 {
		t.Fatalf("caller cache stats = %#v, want one miss and entry", got)
	}

	disabled, err := hatStorage.NewSQLAdapterRegistryWithOptions(hatStorage.SQLAdapterRegistryOptions{DisableCompiledCache: true}, adapter)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := disabled.Execute(context.Background(), "remote", "SELECT * FROM CACHE('items')", nil, hatSql.SQLQueryOptions{}); err != nil {
		t.Fatal(err)
	}
	if got := disabled.CompiledQueryCacheStats(); got != (hatSql.SQLCompiledQueryCacheStats{}) {
		t.Fatalf("disabled compiled cache stats = %#v, want zero", got)
	}
	if _, err := hatStorage.NewSQLAdapterRegistryWithOptions(hatStorage.SQLAdapterRegistryOptions{
		CompiledCache:        cache,
		DisableCompiledCache: true,
	}, adapter); !errors.Is(err, hatStorage.ErrSQLAdapterRegistryOptionsInvalid) {
		t.Fatalf("contradictory cache options error = %v, want invalid-options error", err)
	}
}
