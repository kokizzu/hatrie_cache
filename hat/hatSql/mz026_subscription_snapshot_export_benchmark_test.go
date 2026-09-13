package hatSql

import (
	"context"
	"testing"
)

var mz026CurrentSnapshotSink QuerySubscriptionSnapshot
var mz026ExportedSnapshotsSink []QuerySubscriptionSnapshot

func BenchmarkQuerySubscriptionsSnapshotExport(b *testing.B) {
	resolver := &mz026HistoricalResolver{history: map[uint64][]Row{
		10: {{"id": int64(1), "name": "Ada"}},
		20: {{"id": int64(1), "name": "Lin"}},
	}}
	registry := NewQuerySubscriptions(2)
	for index := 0; index < 8; index++ {
		subscription, err := registry.Subscribe(context.Background(), QuerySubscriptionDefinition{
			Query:        "FROM CACHE('people') SELECT id, name",
			Dependencies: []string{"people"},
			AsOf:         10,
		}, resolver, QueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(subscription.Close)
	}
	registry.mu.RLock()
	subscriptions := make([]*QuerySubscription, 0, len(registry.subs))
	for _, subscription := range registry.subs {
		subscriptions = append(subscriptions, subscription)
	}
	registry.mu.RUnlock()

	b.Run("current-snapshots", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			for _, subscription := range subscriptions {
				snapshot, ok := subscription.Snapshot()
				if !ok {
					b.Fatal("subscription snapshot unavailable")
				}
				mz026CurrentSnapshotSink = snapshot
			}
		}
	})

	b.Run("exact-frontier-export", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			exported, err := registry.ExportSnapshotsAt(context.Background(), 20, resolver, QueryOptions{})
			if err != nil {
				b.Fatal(err)
			}
			mz026ExportedSnapshotsSink = exported
		}
	})
}
