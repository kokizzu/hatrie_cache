package hatStorage_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
	"hatrie_cache/hat/hatStorage"
)

func BenchmarkSQLAdapterRegistryExecuteLocal(b *testing.B) {
	registry, err := hatStorage.NewSQLAdapterRegistry(nil, hatStorage.SQLNamespaceAdapter{
		NamespaceName: "local",
		Store:         testEngine{},
		Resolver: hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
			return nil, nil
		}),
	})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := registry.Execute(ctx, "local", "SELECT * FROM CACHE('items')", nil, hatSql.SQLQueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLAdapterRegistryExecuteResolverOnly(b *testing.B) {
	registry, err := hatStorage.NewSQLAdapterRegistry(nil, hatStorage.SQLResolverAdapter{
		NamespaceName: "remote",
		Resolver: hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
			return nil, nil
		}),
	})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := registry.Execute(ctx, "remote", "SELECT * FROM CACHE('items')", nil, hatSql.SQLQueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}
