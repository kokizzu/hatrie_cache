package hatSql

import (
	"context"
	"testing"
)

func BenchmarkMZ025IdleNotifyChangedAtProgress(b *testing.B) {
	registry, resolver := benchmarkMZ025Subscriptions(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := registry.NotifyChangedAt(context.Background(), uint64(i+1), nil, resolver, QueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMZ025Heartbeat(b *testing.B) {
	registry, _ := benchmarkMZ025Subscriptions(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := registry.Heartbeat(uint64(i + 1)); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkMZ025Subscriptions(b *testing.B) (*QuerySubscriptions, SourceResolver) {
	b.Helper()
	resolver := SourceResolverFunc(func(_ string, key string) ([]Row, error) {
		return []Row{{"name": key}}, nil
	})
	registry := NewQuerySubscriptions(1)
	for i := 0; i < 64; i++ {
		if _, err := registry.Subscribe(context.Background(), QuerySubscriptionDefinition{
			Query:        "FROM CACHE('people') SELECT name",
			Dependencies: []string{"people"},
			EmitProgress: true,
		}, resolver, QueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
	b.Cleanup(func() {
		for _, subscription := range registry.subscriptionsForBenchmark() {
			subscription.Close()
		}
	})
	return registry, resolver
}

func (registry *QuerySubscriptions) subscriptionsForBenchmark() []*QuerySubscription {
	registry.mu.RLock()
	subscriptions := make([]*QuerySubscription, 0, len(registry.subs))
	for _, subscription := range registry.subs {
		subscriptions = append(subscriptions, subscription)
	}
	registry.mu.RUnlock()
	return subscriptions
}
