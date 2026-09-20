package hatReplication

import (
	"context"
	"testing"
)

const tu39BenchmarkRetainedEvents = 4096

func BenchmarkTU39SpaceChangefeedPublish(b *testing.B) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{Space: "orders", SchemaVersion: "v1", MaxEvents: 4096})
	if err != nil {
		b.Fatal(err)
	}
	event := SpaceChangefeedEvent{Operation: SpaceChangefeedUpsert, Key: []byte("order-1"), After: []byte(`{"status":"paid"}`)}
	for index := 0; index < tu39BenchmarkRetainedEvents; index++ {
		if _, err := feed.Publish(event); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := feed.Publish(event); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU39SpaceChangefeedPublishWithSubscriber(b *testing.B) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{Space: "orders", SchemaVersion: "v1", MaxEvents: 4096})
	if err != nil {
		b.Fatal(err)
	}
	subscription, err := feed.Subscribe(context.Background(), SpaceChangefeedSubscribeOptions{Buffer: 1})
	if err != nil {
		b.Fatal(err)
	}
	defer subscription.Close()
	event := SpaceChangefeedEvent{Operation: SpaceChangefeedUpsert, Key: []byte("order-1"), After: []byte(`{"status":"paid"}`)}
	for index := 0; index < tu39BenchmarkRetainedEvents; index++ {
		if _, err := feed.Publish(event); err != nil {
			b.Fatal(err)
		}
		<-subscription.Events()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := feed.Publish(event); err != nil {
			b.Fatal(err)
		}
		<-subscription.Events()
	}
}
