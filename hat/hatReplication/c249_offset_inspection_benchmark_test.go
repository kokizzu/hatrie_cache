package hatReplication

import (
	"context"
	"testing"
)

func c249BenchmarkFeed(b *testing.B) *SpaceChangefeed {
	b.Helper()
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{
		Space:         "orders",
		SchemaVersion: "v1",
		MaxEvents:     2048,
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 1024; index++ {
		if _, err := feed.Publish(SpaceChangefeedEvent{
			Operation: SpaceChangefeedUpsert,
			Key:       []byte("order-key"),
			After:     []byte("order-value"),
		}); err != nil {
			b.Fatal(err)
		}
	}
	return feed
}

func BenchmarkC249ExistingSubscriptionInspection(b *testing.B) {
	feed := c249BenchmarkFeed(b)
	checkpoint := ChangefeedCheckpoint{Source: "orders", Sequence: 1000}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		subscription, err := feed.Subscribe(context.Background(), SpaceChangefeedSubscribeOptions{
			Checkpoint: checkpoint,
			Buffer:     32,
		})
		if err != nil {
			b.Fatal(err)
		}
		for eventIndex := 0; eventIndex < 24; eventIndex++ {
			if _, ok := <-subscription.Events(); !ok {
				b.Fatal("subscription closed before replay completed")
			}
		}
		subscription.Close()
	}
}

func BenchmarkC249ReadOnlyOffsetInspection(b *testing.B) {
	feed := c249BenchmarkFeed(b)
	options := SpaceChangefeedInspectOptions{AfterSequence: 1000, MaxEvents: 24}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		inspection, err := feed.Inspect(context.Background(), options)
		if err != nil {
			b.Fatal(err)
		}
		if len(inspection.Events) != options.MaxEvents {
			b.Fatalf("Inspect() returned %d events, want %d", len(inspection.Events), options.MaxEvents)
		}
	}
}
