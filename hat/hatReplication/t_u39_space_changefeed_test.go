package hatReplication

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestTU39SpaceChangefeedPublishesSchemaBoundEventsAndCheckpoints(t *testing.T) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{
		Space:         "orders",
		SchemaVersion: 7,
		MaxEvents:     4,
		MaxBytes:      2048,
	})
	if err != nil {
		t.Fatalf("NewSpaceChangefeed() error = %v", err)
	}
	subscription, err := feed.Subscribe(SpaceChangefeedSubscribeOptions{})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	key := []byte("order-1")
	after := []byte(`{"status":"paid"}`)
	sequence, err := feed.Publish(SpaceChangefeedInput{
		Operation: SpaceChangefeedInsert,
		Key:       key,
		After:     after,
	})
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	key[0] = 'X'
	after[0] = 'X'
	if sequence != 1 {
		t.Fatalf("Publish() sequence = %d, want 1", sequence)
	}

	events, err := subscription.Read(context.Background(), 8)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("Read() returned %d events, want 1", len(events))
	}
	if events[0].Space != "orders" || events[0].SchemaVersion != 7 || events[0].Sequence != 1 {
		t.Fatalf("event identity = %#v", events[0])
	}
	if events[0].Operation != SpaceChangefeedInsert || !bytes.Equal(events[0].Key, []byte("order-1")) || !bytes.Equal(events[0].After, []byte(`{"status":"paid"}`)) {
		t.Fatalf("event payload = %#v", events[0])
	}
	if err := subscription.Ack(1); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}

	checkpoint, err := subscription.Checkpoint()
	if err != nil {
		t.Fatalf("Checkpoint() error = %v", err)
	}
	encoded, err := checkpoint.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	decoded, err := UnmarshalSpaceChangefeedCheckpoint(encoded)
	if err != nil {
		t.Fatalf("UnmarshalSpaceChangefeedCheckpoint() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, checkpoint) {
		t.Fatalf("decoded checkpoint = %#v, want %#v", decoded, checkpoint)
	}

	resumed, err := feed.Subscribe(SpaceChangefeedSubscribeOptions{Checkpoint: &decoded})
	if err != nil {
		t.Fatalf("Subscribe(checkpoint) error = %v", err)
	}
	if _, err := feed.Publish(SpaceChangefeedInput{Operation: SpaceChangefeedUpdate, Key: []byte("order-1"), Before: []byte(`{"status":"paid"}`), After: []byte(`{"status":"shipped"}`)}); err != nil {
		t.Fatalf("second Publish() error = %v", err)
	}
	resumedEvents, err := resumed.Read(context.Background(), 8)
	if err != nil {
		t.Fatalf("resumed Read() error = %v", err)
	}
	if len(resumedEvents) != 1 || resumedEvents[0].Sequence != 2 || resumedEvents[0].Operation != SpaceChangefeedUpdate {
		t.Fatalf("resumed events = %#v", resumedEvents)
	}
}

func TestTU39SpaceChangefeedBackpressureAndGapRecovery(t *testing.T) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{Space: "items", SchemaVersion: 1, MaxEvents: 2, MaxBytes: 1024})
	if err != nil {
		t.Fatalf("NewSpaceChangefeed() error = %v", err)
	}
	subscription, err := feed.Subscribe(SpaceChangefeedSubscribeOptions{})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	for index := 0; index < 2; index++ {
		if _, err := feed.Publish(SpaceChangefeedInput{Operation: SpaceChangefeedInsert, Key: []byte{byte('a' + index)}}); err != nil {
			t.Fatalf("Publish(%d) error = %v", index, err)
		}
	}
	if _, err := feed.Publish(SpaceChangefeedInput{Operation: SpaceChangefeedInsert, Key: []byte("c")}); !errors.Is(err, ErrSpaceChangefeedBackpressure) {
		t.Fatalf("third Publish() error = %v, want ErrSpaceChangefeedBackpressure", err)
	}
	events, err := subscription.Read(context.Background(), 1)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(events) != 1 || events[0].Sequence != 1 {
		t.Fatalf("first events = %#v", events)
	}
	if err := subscription.Ack(1); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
	if _, err := feed.Publish(SpaceChangefeedInput{Operation: SpaceChangefeedInsert, Key: []byte("c")}); err != nil {
		t.Fatalf("Publish() after Ack error = %v", err)
	}

	late, err := feed.Subscribe(SpaceChangefeedSubscribeOptions{StartSequence: 0})
	if err != nil {
		t.Fatalf("late Subscribe() error = %v", err)
	}
	if _, err := late.Read(context.Background(), 8); !errors.Is(err, ErrSpaceChangefeedGap) {
		t.Fatalf("late Read() error = %v, want ErrSpaceChangefeedGap", err)
	}
}

func TestTU39SpaceChangefeedBatchIsAtomicAndSchemaBound(t *testing.T) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{Space: "users", SchemaVersion: 2, MaxEvents: 2, MaxBytes: 512})
	if err != nil {
		t.Fatalf("NewSpaceChangefeed() error = %v", err)
	}
	batch := []SpaceChangefeedInput{
		{Operation: SpaceChangefeedInsert, Key: []byte("1")},
		{Operation: SpaceChangefeedInsert, Key: []byte("2")},
		{Operation: SpaceChangefeedInsert, Key: []byte("3")},
	}
	if _, err := feed.PublishBatch(batch); !errors.Is(err, ErrSpaceChangefeedBackpressure) {
		t.Fatalf("oversized batch error = %v, want ErrSpaceChangefeedBackpressure", err)
	}
	if feed.LastSequence() != 0 {
		t.Fatalf("LastSequence() = %d after rejected batch, want 0", feed.LastSequence())
	}
	checkpoint := SpaceChangefeedCheckpoint{Space: "other", SchemaVersion: 2, Sequence: 0}
	if _, err := feed.Subscribe(SpaceChangefeedSubscribeOptions{Checkpoint: &checkpoint}); !errors.Is(err, ErrSpaceChangefeedCheckpointMismatch) {
		t.Fatalf("foreign checkpoint error = %v, want ErrSpaceChangefeedCheckpointMismatch", err)
	}
	if _, err := feed.PublishBatch([]SpaceChangefeedInput{{Operation: SpaceChangefeedInsert, Key: []byte("1")}, {Operation: SpaceChangefeedInsert, Key: []byte("2")}}); err != nil {
		t.Fatalf("valid batch error = %v", err)
	}
}

func TestTU39SpaceChangefeedWaitHonorsContextAndClose(t *testing.T) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{Space: "events", SchemaVersion: 1, MaxEvents: 4, MaxBytes: 512})
	if err != nil {
		t.Fatalf("NewSpaceChangefeed() error = %v", err)
	}
	subscription, err := feed.Subscribe(SpaceChangefeedSubscribeOptions{})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := subscription.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait() error = %v, want context deadline", err)
	}
	if _, err := feed.Publish(SpaceChangefeedInput{Operation: SpaceChangefeedDelete, Key: []byte("1")}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if err := subscription.Wait(context.Background()); err != nil {
		t.Fatalf("Wait() after publish error = %v", err)
	}
	subscription.Close()
	if err := subscription.Wait(context.Background()); !errors.Is(err, ErrSpaceChangefeedClosed) {
		t.Fatalf("Wait() after Close error = %v, want ErrSpaceChangefeedClosed", err)
	}
}

func TestTU39SpaceChangefeedRejectsMalformedCheckpoints(t *testing.T) {
	if _, err := UnmarshalSpaceChangefeedCheckpoint([]byte("bad")); !errors.Is(err, ErrSpaceChangefeedCheckpointInvalid) {
		t.Fatalf("malformed checkpoint error = %v, want ErrSpaceChangefeedCheckpointInvalid", err)
	}
	if _, err := NewSpaceChangefeed(SpaceChangefeedOptions{Space: " ", SchemaVersion: 1}); !errors.Is(err, ErrSpaceChangefeedInvalid) {
		t.Fatalf("blank space error = %v, want ErrSpaceChangefeedInvalid", err)
	}
}
