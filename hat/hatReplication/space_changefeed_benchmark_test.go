package hatReplication

import (
	"context"
	"testing"
	"time"
)

var spaceChangefeedBenchmarkSink SpaceChangefeedEvent

func BenchmarkSpaceChangefeedPublish(b *testing.B) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{
		Space:        "orders",
		SchemaVersion: 1,
		Capacity:     3,
		Now:          func() time.Time { return time.Unix(1700000000, 0) },
	})
	if err != nil {
		b.Fatal(err)
	}
	event := SpaceChangefeedEvent{
		Space:         "orders",
		SchemaVersion: 1,
		Operation: "upsert",
		Key:       []byte("order:42"),
		Value:     []byte("value"),
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		published, err := feed.Publish(ctx, event)
		if err != nil {
			b.Fatal(err)
		}
		spaceChangefeedBenchmarkSink = published
	}
}

func BenchmarkSpaceChangefeedPublishReceive(b *testing.B) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{
		Space:        "orders",
		SchemaVersion: 1,
		Capacity:     1,
		Now:          func() time.Time { return time.Unix(1700000000, 0) },
	})
	if err != nil {
		b.Fatal(err)
	}
	checkpoint, err := NewSpaceChangefeedCheckpoint("orders", 1, 0)
	if err != nil {
		b.Fatal(err)
	}
	subscription, err := feed.Subscribe(checkpoint, 1)
	if err != nil {
		b.Fatal(err)
	}
	event := SpaceChangefeedEvent{
		Space:         "orders",
		SchemaVersion: 1,
		Operation: "upsert",
		Key:       []byte("order:42"),
		Value:     []byte("value"),
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := feed.Publish(ctx, event); err != nil {
			b.Fatal(err)
		}
		received, err := subscription.Receive(ctx)
		if err != nil {
			b.Fatal(err)
		}
		spaceChangefeedBenchmarkSink = received
	}
}
