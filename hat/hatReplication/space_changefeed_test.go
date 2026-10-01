package hatReplication_test

import (
	"context"
	"errors"
	"runtime"
	"testing"

	"hatrie_cache/hat/hatReplication"
)

func TestSpaceChangefeedPublishesOrderedReplayableChanges(t *testing.T) {
	feed, err := hatReplication.NewSpaceChangefeed(hatReplication.SpaceChangefeedOptions{
		Space:         " orders ",
		SchemaVersion: 7,
		Capacity:      4,
	})
	if err != nil {
		t.Fatalf("NewSpaceChangefeed() error = %v", err)
	}
	initial := feed.InitialCheckpoint()
	subscription, err := feed.Subscribe(initial)
	if err != nil {
		t.Fatalf("Subscribe(initial) error = %v", err)
	}

	last, err := feed.Publish(context.Background(), []hatReplication.SpaceChangefeedChange{
		spaceChange("1", hatReplication.SpaceChangefeedCreate, nil, []byte("new")),
		spaceChange("2", hatReplication.SpaceChangefeedUpdate, []byte("new"), []byte("paid")),
	})
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if last.Sequence != 2 || last.Space != "orders" || last.SchemaVersion != 7 {
		t.Fatalf("last checkpoint = %#v, want orders/v7/2", last)
	}

	first, err := subscription.Next(context.Background())
	if err != nil {
		t.Fatalf("Next(first) error = %v", err)
	}
	second, err := subscription.Next(context.Background())
	if err != nil {
		t.Fatalf("Next(second) error = %v", err)
	}
	if first.Checkpoint.Sequence != 1 || second.Checkpoint.Sequence != 2 || first.Change.Operation != hatReplication.SpaceChangefeedCreate || second.Change.Operation != hatReplication.SpaceChangefeedUpdate {
		t.Fatalf("ordered events = %#v/%#v", first, second)
	}
	first.Change.After[0] = 'x'
	if err := subscription.Ack(first.Checkpoint.Sequence); err != nil {
		t.Fatalf("Ack(first) error = %v", err)
	}
	checkpoint := subscription.Checkpoint()
	if checkpoint.Sequence != 1 {
		t.Fatalf("subscription checkpoint = %#v, want sequence 1", checkpoint)
	}
	if err := subscription.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reconnected, err := feed.Subscribe(checkpoint)
	if err != nil {
		t.Fatalf("Subscribe(checkpoint) error = %v", err)
	}
	replayed, err := reconnected.Next(context.Background())
	if err != nil {
		t.Fatalf("Next(replayed) error = %v", err)
	}
	if replayed.Checkpoint.Sequence != 2 || string(replayed.Change.After) != "paid" {
		t.Fatalf("replayed event = %#v, want original sequence 2/after paid", replayed)
	}
	if err := reconnected.Ack(replayed.Checkpoint.Sequence); err != nil {
		t.Fatalf("Ack(replayed) error = %v", err)
	}
	if got := reconnected.Checkpoint(); got != last {
		t.Fatalf("reconnected checkpoint = %#v, want %#v", got, last)
	}
}

func TestSpaceChangefeedSchemaAndContextBackpressure(t *testing.T) {
	feed, err := hatReplication.NewSpaceChangefeed(hatReplication.SpaceChangefeedOptions{Space: "orders", SchemaVersion: 3, Capacity: 1})
	if err != nil {
		t.Fatalf("NewSpaceChangefeed() error = %v", err)
	}
	if _, err := feed.Subscribe(hatReplication.SpaceChangefeedCheckpoint{Space: "orders", SchemaVersion: 4}); !errors.Is(err, hatReplication.ErrSpaceChangefeedSchemaMismatch) {
		t.Fatalf("schema mismatch error = %v, want ErrSpaceChangefeedSchemaMismatch", err)
	}
	subscription, err := feed.Subscribe(feed.InitialCheckpoint())
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	if _, err := feed.Publish(context.Background(), []hatReplication.SpaceChangefeedChange{spaceChange("1", hatReplication.SpaceChangefeedCreate, nil, []byte("new"))}); err != nil {
		t.Fatalf("first Publish() error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := feed.Publish(canceled, []hatReplication.SpaceChangefeedChange{spaceChange("2", hatReplication.SpaceChangefeedCreate, nil, []byte("new"))}); !errors.Is(err, context.Canceled) {
		t.Fatalf("full Publish() error = %v, want context.Canceled", err)
	}
	event, err := subscription.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if err := subscription.Ack(event.Checkpoint.Sequence); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
	if _, err := feed.Publish(context.Background(), []hatReplication.SpaceChangefeedChange{spaceChange("2", hatReplication.SpaceChangefeedCreate, nil, []byte("new"))}); err != nil {
		t.Fatalf("Publish(after ack) error = %v", err)
	}
}

func TestSpaceChangefeedReportsHistoryGapAndClose(t *testing.T) {
	feed, err := hatReplication.NewSpaceChangefeed(hatReplication.SpaceChangefeedOptions{Space: "orders", SchemaVersion: 1, Capacity: 2})
	if err != nil {
		t.Fatalf("NewSpaceChangefeed() error = %v", err)
	}
	if _, err := feed.Publish(context.Background(), []hatReplication.SpaceChangefeedChange{
		spaceChange("1", hatReplication.SpaceChangefeedCreate, nil, []byte("a")),
		spaceChange("2", hatReplication.SpaceChangefeedCreate, nil, []byte("b")),
	}); err != nil {
		t.Fatalf("first Publish() error = %v", err)
	}
	last, err := feed.Publish(context.Background(), []hatReplication.SpaceChangefeedChange{
		spaceChange("3", hatReplication.SpaceChangefeedCreate, nil, []byte("c")),
	})
	if err != nil {
		t.Fatalf("second Publish() error = %v", err)
	}
	if _, err := feed.Subscribe(feed.InitialCheckpoint()); !errors.Is(err, hatReplication.ErrSpaceChangefeedHistoryGone) {
		t.Fatalf("stale checkpoint error = %v, want ErrSpaceChangefeedHistoryGone", err)
	}
	subscription, err := feed.Subscribe(hatReplication.SpaceChangefeedCheckpoint{Space: "orders", SchemaVersion: 1, Sequence: 1})
	if err != nil {
		t.Fatalf("Subscribe(sequence 1) error = %v", err)
	}
	if err := feed.Close(); err != nil {
		t.Fatalf("feed Close() error = %v", err)
	}
	for sequence := uint64(2); sequence <= last.Sequence; sequence++ {
		event, err := subscription.Next(context.Background())
		if err != nil {
			t.Fatalf("Next(sequence %d) error = %v", sequence, err)
		}
		if event.Checkpoint.Sequence != sequence {
			t.Fatalf("event sequence = %d, want %d", event.Checkpoint.Sequence, sequence)
		}
		if err := subscription.Ack(sequence); err != nil {
			t.Fatalf("Ack(sequence %d) error = %v", sequence, err)
		}
	}
	if _, err := subscription.Next(context.Background()); !errors.Is(err, hatReplication.ErrSpaceChangefeedClosed) {
		t.Fatalf("closed Next() error = %v, want ErrSpaceChangefeedClosed", err)
	}
}

func TestSpaceChangefeedRejectsInvalidOptions(t *testing.T) {
	var zero hatReplication.SpaceChangefeed
	if _, err := zero.Subscribe(hatReplication.SpaceChangefeedCheckpoint{}); !errors.Is(err, hatReplication.ErrSpaceChangefeedInvalid) {
		t.Fatalf("zero-value Subscribe() error = %v, want ErrSpaceChangefeedInvalid", err)
	}
	for _, options := range []hatReplication.SpaceChangefeedOptions{
		{Space: "", SchemaVersion: 1, Capacity: 1},
		{Space: "orders", SchemaVersion: 0, Capacity: 1},
		{Space: "orders", SchemaVersion: 1, Capacity: -1},
	} {
		if _, err := hatReplication.NewSpaceChangefeed(options); err == nil {
			t.Fatalf("NewSpaceChangefeed(%#v) succeeded, want error", options)
		}
	}
	feed, err := hatReplication.NewSpaceChangefeed(hatReplication.SpaceChangefeedOptions{Space: "orders", SchemaVersion: 1, Capacity: 1})
	if err != nil {
		t.Fatalf("NewSpaceChangefeed() error = %v", err)
	}
	if _, err := feed.Publish(context.Background(), []hatReplication.SpaceChangefeedChange{{Operation: hatReplication.SpaceChangefeedCreate, After: make([]byte, hatReplication.MaxSpaceChangefeedChangeBytes+1)}}); !errors.Is(err, hatReplication.ErrSpaceChangefeedPayloadTooLarge) {
		t.Fatalf("oversized payload error = %v, want ErrSpaceChangefeedPayloadTooLarge", err)
	}
}

func spaceChange(key string, operation hatReplication.SpaceChangefeedOperation, before, after []byte) hatReplication.SpaceChangefeedChange {
	return hatReplication.SpaceChangefeedChange{
		Key:       []byte(key),
		Before:    before,
		After:     after,
		Operation: operation,
	}
}

func BenchmarkSpaceChangefeedPublishAndNext(b *testing.B) {
	feed, err := hatReplication.NewSpaceChangefeed(hatReplication.SpaceChangefeedOptions{Space: "orders", SchemaVersion: 1, Capacity: 1024})
	if err != nil {
		b.Fatal(err)
	}
	subscription, err := feed.Subscribe(feed.InitialCheckpoint())
	if err != nil {
		b.Fatal(err)
	}
	change := spaceChange("1", hatReplication.SpaceChangefeedUpdate, []byte("new"), []byte("paid"))
	batch := []hatReplication.SpaceChangefeedChange{change}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := feed.Publish(context.Background(), batch); err != nil {
			b.Fatal(err)
		}
		event, err := subscription.Next(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		if err := subscription.Ack(event.Checkpoint.Sequence); err != nil {
			b.Fatal(err)
		}
	}
}

var benchmarkSpaceChangefeedControlSink hatReplication.SpaceChangefeedChange

func BenchmarkSpaceChangefeedDirectControl(b *testing.B) {
	change := spaceChange("1", hatReplication.SpaceChangefeedUpdate, []byte("new"), []byte("paid"))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchmarkSpaceChangefeedControlSink = change
	}
}

func TestSpaceChangefeedRetainedMemory(t *testing.T) {
	const eventCount = 1024
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	feed, err := hatReplication.NewSpaceChangefeed(hatReplication.SpaceChangefeedOptions{Space: "orders", SchemaVersion: 1, Capacity: eventCount})
	if err != nil {
		t.Fatalf("NewSpaceChangefeed() error = %v", err)
	}
	for i := 0; i < eventCount; i++ {
		if _, err := feed.Publish(context.Background(), []hatReplication.SpaceChangefeedChange{{Key: []byte("id"), Operation: hatReplication.SpaceChangefeedCreate, After: []byte("payload-32-bytes-xxxxxxxxxxxxxxxx")}}); err != nil {
			t.Fatalf("Publish(%d) error = %v", i, err)
		}
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	var retained uint64
	if after.HeapAlloc > before.HeapAlloc {
		retained = after.HeapAlloc - before.HeapAlloc
	}
	t.Logf("events=%d retained_heap_bytes=%d retained_bytes_per_event=%.1f", eventCount, retained, float64(retained)/eventCount)
	if got := feed.CurrentCheckpoint().Sequence; got != eventCount {
		t.Fatalf("CurrentCheckpoint().Sequence = %d, want %d", got, eventCount)
	}
}
