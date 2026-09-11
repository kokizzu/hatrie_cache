package hatSql_test

import (
	"context"
	"sync/atomic"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkMZ019NamespaceGovernorLegacyConcurrent(b *testing.B) {
	governor, err := hatSql.NewNamespaceQueryGovernor(hatSql.NamespaceResourceLimits{}, nil)
	if err != nil {
		b.Fatal(err)
	}
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"id": int64(1)}}, nil
	})
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			result, err := governor.Execute(context.Background(), "default", "SELECT id FROM CACHE('items')", resolver, nil, hatSql.QueryOptions{})
			if err != nil {
				b.Errorf("execute query: %v", err)
				return
			}
			if len(result.Rows) != 1 {
				b.Errorf("rows = %d, want 1", len(result.Rows))
				return
			}
		}
	})
}

func BenchmarkMZ019NamespaceGovernorNamedPoolsConcurrent(b *testing.B) {
	governor, err := hatSql.NewNamespaceQueryGovernor(
		hatSql.NamespaceResourceLimits{},
		map[string]hatSql.NamespaceResourceLimits{
			"east": {ComputeWorkers: 2, ComputeQueueCapacity: 64},
			"west": {ComputeWorkers: 2, ComputeQueueCapacity: 64},
		},
	)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := governor.Close(); err != nil {
			b.Fatal(err)
		}
	}()
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"id": int64(1)}}, nil
	})
	var sequence atomic.Uint64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			namespace := "east"
			if sequence.Add(1)%2 == 0 {
				namespace = "west"
			}
			result, err := governor.Execute(context.Background(), namespace, "SELECT id FROM CACHE('items')", resolver, nil, hatSql.QueryOptions{})
			if err != nil {
				b.Errorf("execute query: %v", err)
				return
			}
			if len(result.Rows) != 1 {
				b.Errorf("rows = %d, want 1", len(result.Rows))
				return
			}
		}
	})
}
