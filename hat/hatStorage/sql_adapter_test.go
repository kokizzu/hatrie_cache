package hatStorage_test

import (
	"context"
	"errors"
	"testing"

	"hatrie_cache/hat/hatCache"
	"hatrie_cache/hat/hatSql"
	"hatrie_cache/hat/hatStorage"
)

func TestPersistentStoresImplementSQLAdapterEngineContract(t *testing.T) {
	var _ hatStorage.Engine = (*hatCache.LevelDBStore)(nil)
	var _ hatStorage.Engine = (*hatCache.PebbleStore)(nil)
}

func TestSQLAdapterRegistryExecutesThroughRegisteredNamespace(t *testing.T) {
	resolverCalls := 0
	registry, err := hatStorage.NewSQLAdapterRegistry(nil, hatStorage.SQLNamespaceAdapter{
		NamespaceName: "eu",
		Store:         testEngine{},
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
	if _, err := registry.Execute(context.Background(), "eu", "SELECT * FROM CACHE('items')", nil, hatSql.SQLQueryOptions{}); err != nil {
		t.Fatal(err)
	}
	if resolverCalls != 1 {
		t.Fatalf("resolver calls = %d, want 1", resolverCalls)
	}
	inspection, err := registry.Inspect("eu")
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Backend != hatStorage.BackendLevelDB {
		t.Fatalf("inspection backend = %q", inspection.Backend)
	}
	_, err = registry.Execute(context.Background(), "missing", "SELECT * FROM CACHE('items')", nil, hatSql.SQLQueryOptions{})
	if !errors.Is(err, hatStorage.ErrUnknownSQLNamespace) {
		t.Fatalf("unknown namespace error = %v", err)
	}
}
