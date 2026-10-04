package hatReplication

import (
	"errors"
	"sync"
	"testing"
)

func TestSpaceChangefeedPublishesOrderedNamedSpaceEvents(t *testing.T) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{
		Space:         "orders/eu",
		SchemaVersion: 7,
		MaxEvents:     8,
		MaxBytes:      1024,
	})
	if err != nil {
		t.Fatalf("NewSpaceChangefeed() error = %v", err)
	}
	initial := feed.InitialCheckpoint()
	key := []byte("order-1")
	value := []byte("paid")
	first, err := feed.Publish(SpaceChangeInsert, key, value)
	if err != nil {
		t.Fatalf("Publish(insert) error = %v", err)
	}
	key[0] = 'X'
	value[0] = 'X'
	second, err := feed.Publish(SpaceChangeUpdate, []byte("order-1"), []byte("shipped"))
	if err != nil {
		t.Fatalf("Publish(update) error = %v", err)
	}
	if first.Sequence != 1 || second.Sequence != 2 || first.Source != "orders/eu" {
		t.Fatalf("checkpoints = %#v, %#v", first, second)
	}

	changes, next, err := feed.ReadAfter(initial, 1)
	if err != nil {
		t.Fatalf("ReadAfter(first) error = %v", err)
	}
	if len(changes) != 1 || next.Sequence != 1 {
		t.Fatalf("first read = %#v, next = %#v", changes, next)
	}
	if feed.Space() != "orders/eu" || feed.SchemaVersion() != 7 || changes[0].Operation != SpaceChangeInsert {
		t.Fatalf("first change metadata = feed(%q,%d) change(%#v)", feed.Space(), feed.SchemaVersion(), changes[0])
	}
	if string(changes[0].Key) != "order-1" || string(changes[0].Value) != "paid" {
		t.Fatalf("first change payload = %#v", changes[0])
	}

	changes[0].Key[0] = 'Y'
	changes, next, err = feed.ReadAfter(next, 8)
	if err != nil {
		t.Fatalf("ReadAfter(second) error = %v", err)
	}
	if len(changes) != 1 || next.Sequence != 2 || changes[0].Operation != SpaceChangeUpdate {
		t.Fatalf("second read = %#v, next = %#v", changes, next)
	}
	if string(changes[0].Key) != "order-1" || string(changes[0].Value) != "shipped" {
		t.Fatalf("second change payload = %#v", changes[0])
	}
}

func TestSpaceChangefeedDeletesAndCompactsThroughCheckpoint(t *testing.T) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{Space: "orders", MaxEvents: 2, MaxBytes: 128})
	if err != nil {
		t.Fatalf("NewSpaceChangefeed() error = %v", err)
	}
	initial := feed.InitialCheckpoint()
	first, err := feed.Publish(SpaceChangeInsert, []byte("1"), []byte("a"))
	if err != nil {
		t.Fatalf("Publish(insert) error = %v", err)
	}
	second, err := feed.Publish(SpaceChangeDelete, []byte("1"), nil)
	if err != nil {
		t.Fatalf("Publish(delete) error = %v", err)
	}
	if _, err := feed.Publish(SpaceChangeInsert, []byte("2"), []byte("b")); !errors.Is(err, ErrSpaceChangefeedFull) {
		t.Fatalf("full publish error = %v, want ErrSpaceChangefeedFull", err)
	}
	if err := feed.CompactThrough(first); err != nil {
		t.Fatalf("CompactThrough() error = %v", err)
	}
	if _, _, err := feed.ReadAfter(initial, 8); !errors.Is(err, ErrSpaceChangefeedCompacted) {
		t.Fatalf("stale read error = %v, want ErrSpaceChangefeedCompacted", err)
	}
	if _, err := feed.Publish(SpaceChangeInsert, []byte("2"), []byte("b")); err != nil {
		t.Fatalf("Publish(after compact) error = %v", err)
	}
	changes, next, err := feed.ReadAfter(first, 8)
	if err != nil {
		t.Fatalf("ReadAfter(after compact) error = %v", err)
	}
	if len(changes) != 2 || next.Sequence != 3 || changes[0].Operation != SpaceChangeDelete || changes[1].Operation != SpaceChangeInsert {
		t.Fatalf("compacted tail = %#v, next = %#v", changes, next)
	}
	if err := feed.CompactThrough(second); err != nil {
		t.Fatalf("idempotent CompactThrough() error = %v", err)
	}
}

func TestSpaceChangefeedValidatesBoundariesAndCheckpoints(t *testing.T) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{Space: "users", MaxEvents: 2, MaxBytes: 3})
	if err != nil {
		t.Fatalf("NewSpaceChangefeed() error = %v", err)
	}
	if _, err := feed.Publish(SpaceChangeInsert, nil, []byte("x")); !errors.Is(err, ErrSpaceChangefeedKeyRequired) {
		t.Fatalf("empty key error = %v, want ErrSpaceChangefeedKeyRequired", err)
	}
	if _, err := feed.Publish(SpaceChangeOperation(99), []byte("x"), nil); !errors.Is(err, ErrSpaceChangefeedOperationInvalid) {
		t.Fatalf("invalid operation error = %v, want ErrSpaceChangefeedOperationInvalid", err)
	}
	if _, err := feed.Publish(SpaceChangeInsert, []byte("x"), []byte("yzz")); !errors.Is(err, ErrSpaceChangefeedEventTooLarge) {
		t.Fatalf("large event error = %v, want ErrSpaceChangefeedEventTooLarge", err)
	}
	byteFeed, err := NewSpaceChangefeed(SpaceChangefeedOptions{Space: "bytes", MaxEvents: 8, MaxBytes: 3})
	if err != nil {
		t.Fatalf("NewSpaceChangefeed(byte budget) error = %v", err)
	}
	if _, err := byteFeed.Publish(SpaceChangeInsert, []byte("x"), []byte("y")); err != nil {
		t.Fatalf("Publish(byte budget first) error = %v", err)
	}
	if _, err := byteFeed.Publish(SpaceChangeInsert, []byte("z"), []byte("q")); !errors.Is(err, ErrSpaceChangefeedFull) {
		t.Fatalf("byte budget full error = %v, want ErrSpaceChangefeedFull", err)
	}
	checkpoint := feed.InitialCheckpoint()
	if _, _, err := feed.ReadAfter(checkpoint, 0); !errors.Is(err, ErrSpaceChangefeedLimitInvalid) {
		t.Fatalf("invalid limit error = %v, want ErrSpaceChangefeedLimitInvalid", err)
	}
	other, err := NewChangefeedCheckpoint("other", 0)
	if err != nil {
		t.Fatalf("NewChangefeedCheckpoint() error = %v", err)
	}
	if _, _, err := feed.ReadAfter(other, 1); !errors.Is(err, ErrSpaceChangefeedCheckpointSource) {
		t.Fatalf("source mismatch error = %v, want ErrSpaceChangefeedCheckpointSource", err)
	}
	if _, _, err := feed.ReadAfter(ChangefeedCheckpoint{Source: "users", Sequence: 1}, 1); !errors.Is(err, ErrSpaceChangefeedCheckpointFuture) {
		t.Fatalf("future checkpoint error = %v, want ErrSpaceChangefeedCheckpointFuture", err)
	}
}

func TestSpaceChangefeedDefaultsAndOptions(t *testing.T) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{Space: " defaults "})
	if err != nil {
		t.Fatalf("NewSpaceChangefeed() error = %v", err)
	}
	if feed.Space() != "defaults" || feed.SchemaVersion() != 1 {
		t.Fatalf("defaults = space %q schema %d", feed.Space(), feed.SchemaVersion())
	}
	if _, err := NewSpaceChangefeed(SpaceChangefeedOptions{}); !errors.Is(err, ErrSpaceChangefeedSpaceRequired) {
		t.Fatalf("empty space error = %v, want ErrSpaceChangefeedSpaceRequired", err)
	}
	if _, err := NewSpaceChangefeed(SpaceChangefeedOptions{Space: "x", MaxEvents: -1}); !errors.Is(err, ErrSpaceChangefeedOptionsInvalid) {
		t.Fatalf("negative max events error = %v, want ErrSpaceChangefeedOptionsInvalid", err)
	}
}

func TestSpaceChangefeedConcurrentPublishPreservesSequenceOrder(t *testing.T) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{Space: "events", MaxEvents: 1024, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("NewSpaceChangefeed() error = %v", err)
	}
	const publishers = 4
	const eventsPerPublisher = 128
	var group sync.WaitGroup
	group.Add(publishers)
	for publisher := 0; publisher < publishers; publisher++ {
		go func() {
			defer group.Done()
			for event := 0; event < eventsPerPublisher; event++ {
				if _, err := feed.Publish(SpaceChangeInsert, []byte("key"), []byte("value")); err != nil {
					t.Errorf("Publish() error = %v", err)
					return
				}
			}
		}()
	}
	group.Wait()

	changes, next, err := feed.ReadAfter(feed.InitialCheckpoint(), publishers*eventsPerPublisher)
	if err != nil {
		t.Fatalf("ReadAfter() error = %v", err)
	}
	if len(changes) != publishers*eventsPerPublisher || next.Sequence != uint64(len(changes)) {
		t.Fatalf("concurrent read len=%d next=%#v", len(changes), next)
	}
	for index := 1; index < len(changes); index++ {
		if changes[index-1].Sequence >= changes[index].Sequence {
			t.Fatalf("sequence order at %d: %d then %d", index, changes[index-1].Sequence, changes[index].Sequence)
		}
	}
}
