package hatStorage_test

import (
	"context"
	"errors"
	"testing"

	"hatrie_cache/hat/hatSql"
	"hatrie_cache/hat/hatStorage"
)

func TestSQLResolverAdapterExecutesWithoutLocalStorageEngine(t *testing.T) {
	resolverCalls := 0
	registry, err := hatStorage.NewSQLAdapterRegistry(nil, hatStorage.SQLResolverAdapter{
		NamespaceName: "remote",
		Resolver: hatSql.SourceResolverFunc(func(name, key string) ([]hatSql.Row, error) {
			resolverCalls++
			if name != "CACHE" || key != "items" {
				t.Fatalf("resolver source = (%q, %q)", name, key)
			}
			return nil, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Execute(context.Background(), "remote", "SELECT * FROM CACHE('items')", nil, hatSql.SQLQueryOptions{}); err != nil {
		t.Fatal(err)
	}
	if resolverCalls != 1 {
		t.Fatalf("resolver calls = %d, want 1", resolverCalls)
	}
	if _, err := registry.Inspect("remote"); !errors.Is(err, hatStorage.ErrSQLAdapterStorageUnavailable) {
		t.Fatalf("Inspect(remote) error = %v, want ErrSQLAdapterStorageUnavailable", err)
	}
}

func TestSQLNamespaceAdapterStillRequiresStorageEngine(t *testing.T) {
	_, err := hatStorage.NewSQLAdapterRegistry(nil, hatStorage.SQLNamespaceAdapter{
		NamespaceName: "invalid",
		Resolver: hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
			return nil, nil
		}),
	})
	if err == nil {
		t.Fatal("nil storage engine was accepted for SQLNamespaceAdapter")
	}
}
