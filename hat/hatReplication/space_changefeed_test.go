package hatReplication

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSpaceChangefeedReplayCopiesEventsAndCheckpoints(t *testing.T) {
	now := time.Unix(100, 200)
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{
		Space:         "orders",
		SchemaVersion: 7,
		Capacity:      3,
		Now:           func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewSpaceChangefeed() error = %v", err)
	}
	for index := 0; index < 3; index++ {
		key := []byte{byte(index + 1)}
		value := []byte("value")
		if _, err := feed.Publish(context.Background(), SpaceChangefeedEvent{
			Space:         "orders",
			SchemaVersion: 7,
			Operation:     "upsert",
			Key:           key,
			Value:         value,
		}); err != nil {
			t.Fatalf("Publish(%d) error = %v", index, err)
		}
		key[0] = 99
		value[0] = 'X'
	}

	checkpoint, err := NewSpaceChangefeedCheckpoint("orders", 7, 0)
	if err != nil {
		t.Fatalf("NewSpaceChangefeedCheckpoint() error = %v", err)
	}
	subscription, err := feed.Subscribe(checkpoint, 3)
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer subscription.Close()
	for sequence := uint64(1); sequence <= 3; sequence++ {
		event, err := subscription.Receive(context.Background())
		if err != nil {
			t.Fatalf("Receive(%d) error = %v", sequence, err)
		}
		if event.Sequence != sequence || event.At.UnixNano() != now.UnixNano() || string(event.Value) != "value" || event.Key[0] != byte(sequence) {
			t.Fatalf("event = %#v, want sequence %d and copied payload", event, sequence)
		}
	}
	if got := subscription.Checkpoint(); got.Space != "orders" || got.SchemaVersion != 7 || got.Sequence != 3 {
		t.Fatalf("Checkpoint() = %#v", got)
	}

	encoded, err := subscription.Checkpoint().MarshalBinary()
	if err != nil {
		t.Fatalf("checkpoint MarshalBinary() error = %v", err)
	}
	decoded, err := UnmarshalSpaceChangefeedCheckpoint(encoded)
	if err != nil || decoded != subscription.Checkpoint() {
		t.Fatalf("checkpoint round trip = %#v, %v", decoded, err)
	}

	if _, err := feed.Subscribe(SpaceChangefeedCheckpoint{Space: "orders", SchemaVersion: 6}, 3); !errors.Is(err, ErrSpaceChangefeedCheckpointInvalid) {
		t.Fatalf("schema mismatch error = %v, want ErrSpaceChangefeedCheckpointInvalid", err)
	}
	if _, err := feed.Publish(context.Background(), SpaceChangefeedEvent{Space: "orders", SchemaVersion: 6, Operation: "upsert", Key: []byte("x")}); !errors.Is(err, ErrSpaceChangefeedSchemaMismatch) {
		t.Fatalf("publish schema mismatch = %v, want ErrSpaceChangefeedSchemaMismatch", err)
	}
}

func TestSpaceChangefeedBackpressureAndHistoryGap(t *testing.T) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{Space: "orders", SchemaVersion: 1, Capacity: 2})
	if err != nil {
		t.Fatalf("NewSpaceChangefeed() error = %v", err)
	}
	first, err := NewSpaceChangefeedCheckpoint("orders", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := feed.Subscribe(first, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	publish := func(value string) error {
		_, err := feed.Publish(context.Background(), SpaceChangefeedEvent{
			Space:         "orders",
			SchemaVersion: 1,
			Operation:     "upsert",
			Key:           []byte(value),
			Value:         []byte(value),
		})
		return err
	}
	if err := publish("one"); err != nil {
		t.Fatal(err)
	}
	deadline, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if _, err := feed.Publish(deadline, SpaceChangefeedEvent{Space: "orders", SchemaVersion: 1, Operation: "upsert", Key: []byte("two")}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("full subscriber publish = %v, want context.DeadlineExceeded", err)
	}
	if _, err := subscription.Receive(context.Background()); err != nil {
		t.Fatalf("Receive() error = %v", err)
	}
	if err := publish("two"); err != nil {
		t.Fatal(err)
	}
	if _, err := subscription.Receive(context.Background()); err != nil {
		t.Fatalf("Receive(two) error = %v", err)
	}

	if _, err := publishHistory(feed, "three"); err != nil {
		t.Fatal(err)
	}
	if _, err := feed.Subscribe(first, 2); !errors.Is(err, ErrSpaceChangefeedCheckpointExpired) {
		t.Fatalf("expired checkpoint error = %v, want ErrSpaceChangefeedCheckpointExpired", err)
	}
	replayFeed, err := NewSpaceChangefeed(SpaceChangefeedOptions{Space: "orders", SchemaVersion: 1, Capacity: 3})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"one", "two", "three"} {
		if _, err := publishHistory(replayFeed, value); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := replayFeed.Subscribe(SpaceChangefeedCheckpoint{Space: "orders", SchemaVersion: 1, Sequence: 1}, 1); !errors.Is(err, ErrSpaceChangefeedBufferTooSmall) {
		t.Fatalf("small replay buffer error = %v, want ErrSpaceChangefeedBufferTooSmall", err)
	}
	if _, err := replayFeed.Subscribe(SpaceChangefeedCheckpoint{Space: "orders", SchemaVersion: 1, Sequence: 1}, 2); err != nil {
		t.Fatalf("resume subscription error = %v", err)
	}
}

func publishHistory(feed *SpaceChangefeed, value string) (SpaceChangefeedEvent, error) {
	return feed.Publish(context.Background(), SpaceChangefeedEvent{
		Space:         "orders",
		SchemaVersion: 1,
		Operation:     "upsert",
		Key:           []byte(value),
		Value:         []byte(value),
	})
}

func TestSpaceChangefeedRejectsInvalidInputsAndCorruptCheckpoint(t *testing.T) {
	if _, err := NewSpaceChangefeed(SpaceChangefeedOptions{Space: "", SchemaVersion: 1}); !errors.Is(err, ErrSpaceChangefeedInvalid) {
		t.Fatalf("empty space error = %v", err)
	}
	if _, err := NewSpaceChangefeed(SpaceChangefeedOptions{Space: "orders", SchemaVersion: 0}); !errors.Is(err, ErrSpaceChangefeedInvalid) {
		t.Fatalf("zero schema error = %v", err)
	}
	checkpoint, err := NewSpaceChangefeedCheckpoint("orders", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := checkpoint.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	encoded[len(encoded)-1] ^= 1
	if _, err := UnmarshalSpaceChangefeedCheckpoint(encoded); !errors.Is(err, ErrSpaceChangefeedCheckpointInvalid) {
		t.Fatalf("corrupt checkpoint error = %v", err)
	}
}
