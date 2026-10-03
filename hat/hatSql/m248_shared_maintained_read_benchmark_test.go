package hatSql

import (
	"context"
	"testing"
)

type m248BenchmarkResolver struct {
	rows  []Row
	calls int
}

func (resolver *m248BenchmarkResolver) ResolveSQLSource(_ string, _ string) ([]Row, error) {
	resolver.calls++
	return CloneRows(resolver.rows), nil
}

func BenchmarkM248MaintainedReadRefresh(b *testing.B) {
	benchmarkM248MaintainedReadRefresh(b, false)
}

func BenchmarkM248MaintainedReadRefreshShared(b *testing.B) {
	benchmarkM248MaintainedReadRefresh(b, true)
}

func benchmarkM248MaintainedReadRefresh(b *testing.B, share bool) {
	resolver := &m248BenchmarkResolver{rows: make([]Row, 1024)}
	for index := range resolver.rows {
		resolver.rows[index] = Row{"id": int64(index)}
	}
	registry := NewQuerySubscriptions(1)
	definition := QuerySubscriptionDefinition{
		Query:               "FROM CACHE('people') SELECT id",
		Dependencies:        []string{"people"},
		ShareIdenticalReads: share,
	}
	for index := 0; index < 64; index++ {
		subscription, err := registry.Subscribe(context.Background(), definition, resolver, QueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		defer subscription.Close()
	}
	resolver.calls = 0
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		resolver.rows[0]["id"] = int64(index)
		if err := registry.NotifyChanged(context.Background(), []string{"people"}, resolver, QueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(resolver.calls)/float64(b.N), "source_resolves/op")
}
