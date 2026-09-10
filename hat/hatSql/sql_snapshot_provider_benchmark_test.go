package hatSql

import (
	"context"
	"testing"
)

func BenchmarkSQLSnapshotProviderExecution(b *testing.B) {
	legacy := SourceResolverFunc(func(string, string) ([]Row, error) {
		return []Row{{"id": int64(1), "state": "ready"}}, nil
	})
	provider := benchmarkSnapshotProvider{resolver: legacy}
	query := "FROM CACHE('items') WHERE state = 'ready' SELECT id"
	for _, test := range []struct {
		name     string
		resolver SQLSourceResolver
	}{
		{name: "legacy", resolver: legacy},
		{name: "provider", resolver: provider},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := ExecuteSQLQueryParameters(context.Background(), query, test.resolver, nil, SQLQueryOptions{})
				if err != nil || len(result.Rows) != 1 {
					b.Fatalf("ExecuteSQLQueryParameters() = %#v, %v", result.Rows, err)
				}
			}
		})
	}
}

type benchmarkSnapshotProvider struct {
	resolver SQLSourceResolver
}

func (provider benchmarkSnapshotProvider) ResolveSQLSource(name, key string) ([]Row, error) {
	return provider.resolver.ResolveSQLSource(name, key)
}

func (provider benchmarkSnapshotProvider) BeginSQLSnapshot(context.Context) (SQLSourceResolver, func(), error) {
	return provider.resolver, nil, nil
}
