package hatSql

import (
	"context"
	"testing"
)

func BenchmarkMU034HistoricalSubscriptionCheckpoint(b *testing.B) {
	resolver := &mu034HistoricalSubscriptionResolver{history: map[uint64][]Row{
		10: {{"id": int64(1), "name": "Ada"}},
	}}
	definition := mu034SubscriptionDefinition()

	b.Run("legacy_snapshot_close", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			registry := NewQuerySubscriptions(1)
			subscription, err := registry.Subscribe(context.Background(), definition, resolver, QueryOptions{})
			if err != nil {
				b.Fatal(err)
			}
			snapshot, ok := subscription.Snapshot()
			if !ok {
				b.Fatal("Snapshot() unavailable")
			}
			_ = snapshot
			subscription.Close()
		}
	})

	b.Run("acknowledge_close_with_checkpoint", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			registry := NewQuerySubscriptions(1)
			subscription, err := registry.Subscribe(context.Background(), definition, resolver, QueryOptions{})
			if err != nil {
				b.Fatal(err)
			}
			snapshot, ok := subscription.Snapshot()
			if !ok {
				b.Fatal("Snapshot() unavailable")
			}
			if err := subscription.Acknowledge(snapshot); err != nil {
				b.Fatal(err)
			}
			if _, err := subscription.CloseWithCheckpoint(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
