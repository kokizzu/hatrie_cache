package hatReplication

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTU39SpaceChangefeedPublishesVersionedEventsAndResumesFromCheckpoint(t *testing.T) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{
		Space:         "orders",
		SchemaVersion: "orders-v1",
		MaxEvents:     8,
	})
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := feed.Subscribe(context.Background(), SpaceChangefeedSubscribeOptions{Buffer: 4})
	if err != nil {
		t.Fatal(err)
	}
	sequence, err := feed.Publish(SpaceChangefeedEvent{
		Operation: SpaceChangefeedUpsert,
		Key:       []byte("order-1"),
		After:     []byte(`{"status":"paid"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	event := <-subscription.Events()
	if event.Sequence != sequence || event.Space != "orders" || event.SchemaVersion != "orders-v1" {
		t.Fatalf("event metadata = %#v, want sequence=%d orders/orders-v1", event, sequence)
	}
	if string(event.After) != `{"status":"paid"}` {
		t.Fatalf("event payload = %q", event.After)
	}
	checkpoint, err := subscription.Advance(sequence)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.Source != "orders" || checkpoint.Sequence != sequence {
		t.Fatalf("checkpoint = %#v", checkpoint)
	}
	subscription.Close()

	resumed, err := feed.Subscribe(context.Background(), SpaceChangefeedSubscribeOptions{
		Checkpoint: checkpoint,
		Buffer:     4,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Close()
	if _, err := feed.Publish(SpaceChangefeedEvent{
		Operation: SpaceChangefeedDelete,
		Key:       []byte("order-1"),
	}); err != nil {
		t.Fatal(err)
	}
	event = <-resumed.Events()
	if event.Sequence != sequence+1 || event.Operation != SpaceChangefeedDelete {
		t.Fatalf("resumed event = %#v", event)
	}
}

func TestTU39SpaceChangefeedRejectsHistoryGapsAndSchemaMismatches(t *testing.T) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{
		Space:         "orders",
		SchemaVersion: "orders-v1",
		MaxEvents:     2,
	})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 3; index++ {
		if _, err := feed.Publish(SpaceChangefeedEvent{Key: []byte{byte(index)}, Operation: SpaceChangefeedUpsert}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := feed.Subscribe(context.Background(), SpaceChangefeedSubscribeOptions{
		Checkpoint: ChangefeedCheckpoint{Source: "orders", Sequence: 0},
	}); !errors.Is(err, ErrSpaceChangefeedHistoryGap) {
		t.Fatalf("history-gap subscribe error = %v", err)
	}
	if _, err := feed.Subscribe(context.Background(), SpaceChangefeedSubscribeOptions{
		ExpectedSchemaVersion: "orders-v2",
	}); !errors.Is(err, ErrSpaceChangefeedSchemaMismatch) {
		t.Fatalf("schema-mismatch subscribe error = %v", err)
	}
	resumed, err := feed.Subscribe(context.Background(), SpaceChangefeedSubscribeOptions{
		Checkpoint: ChangefeedCheckpoint{Source: "orders", Sequence: 1},
		Buffer:     2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Close()
	for sequence := uint64(2); sequence <= 3; sequence++ {
		event := <-resumed.Events()
		if event.Sequence != sequence {
			t.Fatalf("retained event sequence = %d, want %d", event.Sequence, sequence)
		}
	}
}

func TestTU39SpaceChangefeedOverflowIsExplicitAndNonBlocking(t *testing.T) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{Space: "orders", SchemaVersion: "v1"})
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := feed.Subscribe(context.Background(), SpaceChangefeedSubscribeOptions{Buffer: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := feed.Publish(SpaceChangefeedEvent{Key: []byte("one"), Operation: SpaceChangefeedUpsert}); err != nil {
		t.Fatal(err)
	}
	if _, err := feed.Publish(SpaceChangefeedEvent{Key: []byte("two"), Operation: SpaceChangefeedUpsert}); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(subscription.Err(), ErrSpaceChangefeedOverflow) {
		t.Fatalf("subscription error = %v, want overflow", subscription.Err())
	}
	if stats := feed.Stats(); stats.Subscribers != 0 || stats.OverflowedSubscribers != 1 {
		t.Fatalf("stats after overflow = %#v", stats)
	}
}

func TestTU39SpaceChangefeedValidatesBoundsAndCheckpointOwnership(t *testing.T) {
	if _, err := NewSpaceChangefeed(SpaceChangefeedOptions{}); !errors.Is(err, ErrSpaceChangefeedSpaceRequired) {
		t.Fatalf("empty options error = %v", err)
	}
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{Space: "orders", SchemaVersion: "v1", MaxEventBytes: 4})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := feed.Publish(SpaceChangefeedEvent{Key: []byte("large"), Operation: SpaceChangefeedUpsert}); !errors.Is(err, ErrSpaceChangefeedEventTooLarge) {
		t.Fatalf("oversized event error = %v", err)
	}
	if _, err := feed.Subscribe(context.Background(), SpaceChangefeedSubscribeOptions{
		Checkpoint: ChangefeedCheckpoint{Source: "other", Sequence: 1},
	}); !errors.Is(err, ErrSpaceChangefeedCheckpointSource) {
		t.Fatalf("foreign checkpoint error = %v", err)
	}
}

func TestTU39SpaceChangefeedCopiesPayloadsAndSupportsContextCancellation(t *testing.T) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{Space: "orders", SchemaVersion: "v1"})
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := feed.Subscribe(context.Background(), SpaceChangefeedSubscribeOptions{Buffer: 1})
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("order-1")
	after := []byte("paid")
	if _, err := feed.Publish(SpaceChangefeedEvent{Key: key, After: after, Operation: SpaceChangefeedUpsert}); err != nil {
		t.Fatal(err)
	}
	key[0] = 'X'
	after[0] = 'X'
	event := <-subscription.Events()
	if string(event.Key) != "order-1" || string(event.After) != "paid" {
		t.Fatalf("published payload changed after caller mutation = %#v", event)
	}
	event.Key[0] = 'X'
	event.After[0] = 'X'
	subscription.Close()
	replay, err := feed.Subscribe(context.Background(), SpaceChangefeedSubscribeOptions{Buffer: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Close()
	replayed := <-replay.Events()
	if string(replayed.Key) != "order-1" || string(replayed.After) != "paid" {
		t.Fatalf("retained payload changed after subscriber mutation = %#v", replayed)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancelSubscription, err := feed.Subscribe(ctx, SpaceChangefeedSubscribeOptions{Buffer: 1})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-cancelSubscription.Events():
			if ok {
				continue
			}
			if !errors.Is(cancelSubscription.Err(), context.Canceled) {
				t.Fatalf("context cancellation error = %v", cancelSubscription.Err())
			}
			return
		case <-deadline:
			t.Fatal("context cancellation did not close subscription")
		}
	}
}

func TestTU39SpaceChangefeedBoundsSubscriberCount(t *testing.T) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{
		Space:          "orders",
		SchemaVersion:  "v1",
		MaxSubscribers: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := feed.Subscribe(context.Background(), SpaceChangefeedSubscribeOptions{Buffer: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, err := feed.Subscribe(context.Background(), SpaceChangefeedSubscribeOptions{Buffer: 1}); !errors.Is(err, ErrSpaceChangefeedSubscriberLimit) {
		t.Fatalf("subscriber-limit error = %v", err)
	}
	first.Close()
	second, err := feed.Subscribe(context.Background(), SpaceChangefeedSubscribeOptions{Buffer: 1})
	if err != nil {
		t.Fatal(err)
	}
	second.Close()
}
