package hatReplication

import (
	"context"
	"errors"
	"testing"
)

func TestSpaceChangefeedBackpressureIsOptIn(t *testing.T) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{Space: "orders", SchemaVersion: "v1", MaxEvents: 8})
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := feed.Subscribe(context.Background(), SpaceChangefeedSubscribeOptions{Buffer: 8})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 4; index++ {
		if _, err := feed.Publish(SpaceChangefeedEvent{Operation: SpaceChangefeedUpsert, Key: []byte{byte(index + 1)}}); err != nil {
			t.Fatalf("default-off publish %d error = %v", index, err)
		}
		if _, ok := <-subscription.Events(); !ok {
			t.Fatal("default-off subscription closed")
		}
	}
	if got := feed.Stats().BackpressuredPublishes; got != 0 {
		t.Fatalf("default-off backpressure count = %d", got)
	}
}

func TestSpaceChangefeedBackpressureStopsAheadOfDownstreamFrontier(t *testing.T) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{
		Space:         "orders",
		SchemaVersion: "v1",
		MaxEvents:     8,
		Backpressure: SpaceChangefeedBackpressureOptions{
			Enabled: true,
			MaxLag:  2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := feed.Subscribe(context.Background(), SpaceChangefeedSubscribeOptions{Buffer: 8})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 2; index++ {
		if _, err := feed.Publish(SpaceChangefeedEvent{Operation: SpaceChangefeedUpsert, Key: []byte{byte(index + 1)}}); err != nil {
			t.Fatal(err)
		}
		if _, ok := <-subscription.Events(); !ok {
			t.Fatal("backpressure subscription closed before limit")
		}
	}
	if _, err := feed.Publish(SpaceChangefeedEvent{Operation: SpaceChangefeedUpsert, Key: []byte("blocked")}); !errors.Is(err, ErrSpaceChangefeedBackpressure) {
		t.Fatalf("lagged publish error = %v, want ErrSpaceChangefeedBackpressure", err)
	}
	stats := feed.Stats()
	if stats.BackpressuredPublishes != 1 || stats.DownstreamFrontier != 0 || stats.Published != 2 {
		t.Fatalf("backpressure stats = %+v", stats)
	}
	if _, err := subscription.Advance(2); err != nil {
		t.Fatal(err)
	}
	if _, err := feed.Publish(SpaceChangefeedEvent{Operation: SpaceChangefeedUpsert, Key: []byte("released")}); err != nil {
		t.Fatalf("publish after downstream advance = %v", err)
	}
	if event := <-subscription.Events(); event.Sequence != 3 {
		t.Fatalf("released event sequence = %d, want 3", event.Sequence)
	}
}

func TestSpaceChangefeedBackpressureUsesSlowestSubscriber(t *testing.T) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{
		Space:         "orders",
		SchemaVersion: "v1",
		MaxEvents:     8,
		Backpressure:  SpaceChangefeedBackpressureOptions{Enabled: true, MaxLag: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	fast, err := feed.Subscribe(context.Background(), SpaceChangefeedSubscribeOptions{Buffer: 8})
	if err != nil {
		t.Fatal(err)
	}
	slow, err := feed.Subscribe(context.Background(), SpaceChangefeedSubscribeOptions{Buffer: 8})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := feed.Publish(SpaceChangefeedEvent{Operation: SpaceChangefeedUpsert, Key: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	firstFast := <-fast.Events()
	<-slow.Events()
	if _, err := fast.Advance(firstFast.Sequence); err != nil {
		t.Fatal(err)
	}
	if _, err := feed.Publish(SpaceChangefeedEvent{Operation: SpaceChangefeedUpsert, Key: []byte("two")}); err != nil {
		t.Fatal(err)
	}
	secondFast := <-fast.Events()
	<-slow.Events()
	if _, err := fast.Advance(secondFast.Sequence); err != nil {
		t.Fatal(err)
	}
	if _, err := feed.Publish(SpaceChangefeedEvent{Operation: SpaceChangefeedUpsert, Key: []byte("three")}); !errors.Is(err, ErrSpaceChangefeedBackpressure) {
		t.Fatalf("slow subscriber should apply backpressure, got %v", err)
	}
	if got := feed.Stats().DownstreamFrontier; got != 0 {
		t.Fatalf("minimum downstream frontier = %d, want 0", got)
	}
	if got := feed.Stats().MaxDownstreamLag; got != 2 {
		t.Fatalf("maximum downstream lag = %d, want 2", got)
	}
	if _, err := slow.Advance(2); err != nil {
		t.Fatal(err)
	}
	stats := feed.Stats()
	if stats.DownstreamFrontier != 2 || stats.MaxDownstreamLag != 0 {
		t.Fatalf("frontier after slow advance = %+v", stats)
	}
	if _, err := feed.Publish(SpaceChangefeedEvent{Operation: SpaceChangefeedUpsert, Key: []byte("three-retry")}); err != nil {
		t.Fatalf("publish after slow subscriber advance = %v", err)
	}
}

func TestSpaceChangefeedBackpressureValidatesConfiguration(t *testing.T) {
	if _, err := NewSpaceChangefeed(SpaceChangefeedOptions{
		Space:         "orders",
		SchemaVersion: "v1",
		Backpressure:  SpaceChangefeedBackpressureOptions{Enabled: true, MaxLag: MaxSpaceChangefeedMaxLag + 1},
	}); !errors.Is(err, ErrSpaceChangefeedOptionsInvalid) {
		t.Fatalf("invalid max lag error = %v", err)
	}
}

func TestSpaceChangefeedBackpressureDropsClosedSlowSubscriber(t *testing.T) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{
		Space:         "orders",
		SchemaVersion: "v1",
		Backpressure:  SpaceChangefeedBackpressureOptions{Enabled: true, MaxLag: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	fast, err := feed.Subscribe(context.Background(), SpaceChangefeedSubscribeOptions{Buffer: 4})
	if err != nil {
		t.Fatal(err)
	}
	slow, err := feed.Subscribe(context.Background(), SpaceChangefeedSubscribeOptions{Buffer: 4})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := feed.Publish(SpaceChangefeedEvent{Operation: SpaceChangefeedUpsert, Key: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	first := <-fast.Events()
	<-slow.Events()
	if _, err := fast.Advance(first.Sequence); err != nil {
		t.Fatal(err)
	}
	if got := feed.Stats().DownstreamFrontier; got != 0 {
		t.Fatalf("frontier before close = %d, want 0", got)
	}
	slow.Close()
	stats := feed.Stats()
	if stats.Subscribers != 1 || stats.DownstreamFrontier != 1 || stats.MaxDownstreamLag != 0 {
		t.Fatalf("frontier after close = %+v", stats)
	}
	if _, err := feed.Publish(SpaceChangefeedEvent{Operation: SpaceChangefeedUpsert, Key: []byte("two")}); err != nil {
		t.Fatalf("publish after slow close = %v", err)
	}
}
