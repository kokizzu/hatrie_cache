package hatSql

import (
	"context"
	"testing"
)

func BenchmarkC231NamespaceGovernorDefaultPath(b *testing.B) {
	benchmarkC231NamespaceGovernor(b, NamespaceResourceLimits{}, SQLQueryOptions{})
}

func BenchmarkC231NamespaceGovernorMemoryBudget(b *testing.B) {
	benchmarkC231NamespaceGovernor(b, NamespaceResourceLimits{MaxMemoryBytes: 1 << 30}, SQLQueryOptions{MemoryReservationBytes: 1 << 10})
}

func benchmarkC231NamespaceGovernor(b *testing.B, limits NamespaceResourceLimits, options SQLQueryOptions) {
	governor, err := NewNamespaceQueryGovernor(limits, nil)
	if err != nil {
		b.Fatal(err)
	}
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return []SQLRow{{"id": int64(1)}}, nil
	})
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			result, err := governor.Execute(context.Background(), "default", "SELECT id FROM CACHE('items')", resolver, nil, options)
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

func BenchmarkC231MemoryBudgetAcquireRelease(b *testing.B) {
	budget := newNamespaceQueryMemoryBudget(1<<30, 0)
	b.ReportAllocs()
	for range b.N {
		if err := budget.acquire(context.Background(), 1<<10); err != nil {
			b.Fatal(err)
		}
		budget.release(1 << 10)
	}
}
