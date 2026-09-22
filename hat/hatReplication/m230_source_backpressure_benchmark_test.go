package hatReplication

import (
	"context"
	"errors"
	"testing"
)

func BenchmarkM230SpaceChangefeedPublishLegacy(b *testing.B) {
	benchmarkM230SpaceChangefeedPublish(b, false)
}

func BenchmarkM230SpaceChangefeedPublishWithBackpressure(b *testing.B) {
	benchmarkM230SpaceChangefeedPublish(b, true)
}

func BenchmarkM230SpaceChangefeedBackpressureReject(b *testing.B) {
	feed, subscription := newM230SpaceChangefeedBenchmark(b, true)
	if _, err := feed.Publish(SpaceChangefeedEvent{Operation: SpaceChangefeedUpsert, Key: []byte("seed")}); err != nil {
		b.Fatal(err)
	}
	<-subscription.Events()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := feed.Publish(SpaceChangefeedEvent{Operation: SpaceChangefeedUpsert, Key: []byte("blocked")}); !errors.Is(err, ErrSpaceChangefeedBackpressure) {
			b.Fatalf("publish error = %v, want backpressure", err)
		}
	}
}

func benchmarkM230SpaceChangefeedPublish(b *testing.B, enabled bool) {
	feed, subscription := newM230SpaceChangefeedBenchmark(b, enabled)
	event := SpaceChangefeedEvent{Operation: SpaceChangefeedUpsert, Key: []byte("key")}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		sequence, err := feed.Publish(event)
		if err != nil {
			b.Fatal(err)
		}
		<-subscription.Events()
		if _, err := subscription.Advance(sequence); err != nil {
			b.Fatal(err)
		}
	}
}

func newM230SpaceChangefeedBenchmark(b *testing.B, enabled bool) (*SpaceChangefeed, *SpaceChangefeedSubscription) {
	b.Helper()
	options := SpaceChangefeedOptions{
		Space:         "orders",
		SchemaVersion: "v1",
		MaxEvents:     4096,
	}
	if enabled {
		options.Backpressure = SpaceChangefeedBackpressureOptions{Enabled: true, MaxLag: 1}
	}
	feed, err := NewSpaceChangefeed(options)
	if err != nil {
		b.Fatal(err)
	}
	subscription, err := feed.Subscribe(context.Background(), SpaceChangefeedSubscribeOptions{Buffer: 1})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(feed.Close)
	return feed, subscription
}
