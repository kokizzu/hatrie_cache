package hatDataStructure

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func t039Batch(sequence, frontier uint64, changes ...SpaceChange) SpaceChangefeedBatch {
	return SpaceChangefeedBatch{
		Sequence:      sequence,
		Frontier:      frontier,
		SchemaVersion: 7,
		Changes:       changes,
	}
}

func t039Insert(key, value string) SpaceChange {
	return SpaceChange{Operation: SpaceChangeInsert, Key: []byte(key), After: []byte(value)}
}

func TestT039SpaceChangefeedReplaysAndAcksDetachedBatches(t *testing.T) {
	feed, err := NewSpaceChangefeed("orders", 7, SpaceChangefeedOptions{
		MaxHistoryBatches: 4,
		MaxPendingBatches: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	value := []byte("new")
	if err := feed.Append(t039Batch(1, 10, SpaceChange{Operation: SpaceChangeInsert, Key: []byte("1"), After: value})); err != nil {
		t.Fatal(err)
	}
	value[0] = 'x'
	if err := feed.Append(t039Batch(2, 11, SpaceChange{
		Operation: SpaceChangeUpdate,
		Key:       []byte("1"),
		Before:    []byte("new"),
		After:     []byte("paid"),
	})); err != nil {
		t.Fatal(err)
	}

	subscription, err := feed.Subscribe(context.Background(), SpaceChangefeedCheckpoint{})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	first := <-subscription.Updates()
	second := <-subscription.Updates()
	if first.Sequence != 1 || first.SchemaVersion != 7 || string(first.Changes[0].After) != "new" {
		t.Fatalf("first batch = %#v", first)
	}
	if second.Sequence != 2 || string(second.Changes[0].After) != "paid" {
		t.Fatalf("second batch = %#v", second)
	}
	first.Changes[0].After[0] = 'm'
	replayCheckpoint := SpaceChangefeedCheckpoint{Sequence: 1, Frontier: 10, SchemaVersion: 7}
	if err := subscription.Ack(replayCheckpoint); err != nil {
		t.Fatal(err)
	}
	if got := subscription.Checkpoint(); got != replayCheckpoint {
		t.Fatalf("Checkpoint() = %#v, want %#v", got, replayCheckpoint)
	}

	replay, err := feed.Subscribe(context.Background(), replayCheckpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Close()
	if got := <-replay.Updates(); got.Sequence != 2 || string(got.Changes[0].After) != "paid" {
		t.Fatalf("replayed batch = %#v", got)
	}
}

func TestT039SpaceChangefeedValidatesSequenceSchemaAndBounds(t *testing.T) {
	feed, err := NewSpaceChangefeed("orders", 7, SpaceChangefeedOptions{
		MaxHistoryBatches: 1,
		MaxBatchChanges:   1,
		MaxChangeBytes:    8,
	})
	if err != nil {
		t.Fatal(err)
	}
	invalid := []SpaceChangefeedBatch{
		t039Batch(2, 10, t039Insert("1", "x")),
		{Sequence: 1, Frontier: 10, SchemaVersion: 8},
		{Sequence: 1, Frontier: 10, SchemaVersion: 7, Changes: []SpaceChange{t039Insert("123456789", "x")}},
		{Sequence: 1, Frontier: 10, SchemaVersion: 7, Changes: []SpaceChange{t039Insert("1", "12345678")}},
		{Sequence: 1, Frontier: 10, SchemaVersion: 7, Changes: []SpaceChange{t039Insert("1", "x"), t039Insert("2", "y")}},
	}
	for _, batch := range invalid {
		if err := feed.Append(batch); !errors.Is(err, ErrSpaceChangefeedSequence) &&
			!errors.Is(err, ErrSpaceChangefeedInvalid) && !errors.Is(err, ErrSpaceChangefeedLimit) {
			t.Fatalf("Append(%#v) error = %v", batch, err)
		}
	}
	if err := feed.Append(t039Batch(1, 10, t039Insert("1", "x"))); err != nil {
		t.Fatal(err)
	}
	if err := feed.Append(t039Batch(3, 12, t039Insert("3", "z"))); !errors.Is(err, ErrSpaceChangefeedSequence) {
		t.Fatalf("gap append error = %v", err)
	}
	if err := feed.Append(t039Batch(2, 11, t039Insert("2", "y"))); err != nil {
		t.Fatal(err)
	}
	if err := feed.Append(t039Batch(3, 12, t039Insert("3", "z"))); err != nil {
		t.Fatal(err)
	}
	if _, err := feed.Subscribe(context.Background(), SpaceChangefeedCheckpoint{Sequence: 1, Frontier: 10, SchemaVersion: 7}); !errors.Is(err, ErrSpaceChangefeedCheckpointExpired) {
		t.Fatalf("expired checkpoint error = %v", err)
	}
}

func TestT039SpaceChangefeedBackpressureClosesSlowSubscriber(t *testing.T) {
	feed, err := NewSpaceChangefeed("orders", 7, SpaceChangefeedOptions{MaxPendingBatches: 1})
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := feed.Subscribe(context.Background(), SpaceChangefeedCheckpoint{})
	if err != nil {
		t.Fatal(err)
	}
	if err := feed.Append(t039Batch(1, 1, t039Insert("1", "x"))); err != nil {
		t.Fatal(err)
	}
	if err := feed.Append(t039Batch(2, 2, t039Insert("2", "y"))); !errors.Is(err, ErrSpaceChangefeedBackpressure) {
		t.Fatalf("second append error = %v", err)
	}
	if !errors.Is(subscription.Err(), ErrSpaceChangefeedBackpressure) {
		t.Fatalf("subscription error = %v", subscription.Err())
	}
	subscription.Close()
}

func TestT039SpaceChangefeedCheckpointRoundTripAndAckBounds(t *testing.T) {
	want := SpaceChangefeedCheckpoint{Sequence: 42, Frontier: 99, SchemaVersion: 7}
	encoded, err := want.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var got SpaceChangefeedCheckpoint
	if err := got.UnmarshalBinary(encoded); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("decoded checkpoint = %#v, want %#v", got, want)
	}
	encoded[len(encoded)-1] ^= 1
	if err := got.UnmarshalBinary(encoded); !errors.Is(err, ErrSpaceChangefeedInvalid) {
		t.Fatalf("corrupt checkpoint error = %v", err)
	}

	feed, err := NewSpaceChangefeed("orders", 7, SpaceChangefeedOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := feed.Append(t039Batch(1, 10, t039Insert("1", "x"))); err != nil {
		t.Fatal(err)
	}
	subscription, err := feed.Subscribe(context.Background(), SpaceChangefeedCheckpoint{})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	<-subscription.Updates()
	if err := subscription.Ack(SpaceChangefeedCheckpoint{Sequence: 1, Frontier: 11, SchemaVersion: 7}); !errors.Is(err, ErrSpaceChangefeedSequence) {
		t.Fatalf("ahead frontier ack error = %v", err)
	}
}

func TestT039SpaceChangefeedConcurrentSnapshot(t *testing.T) {
	feed, err := NewSpaceChangefeed("orders", 7, SpaceChangefeedOptions{MaxHistoryBatches: 32})
	if err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	wait.Add(1)
	go func() {
		defer wait.Done()
		for sequence := uint64(1); sequence <= 64; sequence++ {
			if err := feed.Append(t039Batch(sequence, sequence, t039Insert("k", "v"))); err != nil {
				t.Errorf("Append(%d) error = %v", sequence, err)
				return
			}
		}
	}()
	for iteration := 0; iteration < 128; iteration++ {
		snapshot := feed.Snapshot()
		if snapshot.Name != "orders" || snapshot.SchemaVersion != 7 || snapshot.LatestSequence > 64 {
			t.Fatalf("snapshot = %#v", snapshot)
		}
	}
	wait.Wait()
}

func TestT039SpaceChangefeedCompletionAndOptionValidation(t *testing.T) {
	for _, options := range []SpaceChangefeedOptions{
		{MaxHistoryBatches: -1},
		{MaxBatchChanges: -1},
		{MaxPendingBatches: -1},
		{MaxSubscribers: -1},
		{MaxChangeBytes: -1},
	} {
		if _, err := NewSpaceChangefeed("orders", 7, options); !errors.Is(err, ErrSpaceChangefeedInvalid) {
			t.Fatalf("options %#v error = %v", options, err)
		}
	}
	if _, err := NewSpaceChangefeed("", 7, SpaceChangefeedOptions{}); !errors.Is(err, ErrSpaceChangefeedInvalid) {
		t.Fatalf("empty name error = %v", err)
	}
	if _, err := NewSpaceChangefeed("orders", 0, SpaceChangefeedOptions{}); !errors.Is(err, ErrSpaceChangefeedInvalid) {
		t.Fatalf("zero schema error = %v", err)
	}

	feed, err := NewSpaceChangefeed("orders", 7, SpaceChangefeedOptions{})
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := feed.Subscribe(context.Background(), SpaceChangefeedCheckpoint{})
	if err != nil {
		t.Fatal(err)
	}
	if err := feed.Append(SpaceChangefeedBatch{Sequence: 1, Frontier: 1, SchemaVersion: 7, Complete: true}); err != nil {
		t.Fatal(err)
	}
	if batch := <-subscription.Updates(); !batch.Complete {
		t.Fatalf("completion batch = %#v", batch)
	}
	if err := subscription.Err(); err != nil {
		t.Fatalf("completion error = %v", err)
	}
	select {
	case <-subscription.Done():
	default:
		t.Fatal("completion did not close Done")
	}
	if err := feed.Append(SpaceChangefeedBatch{Sequence: 2, Frontier: 2, SchemaVersion: 7}); !errors.Is(err, ErrSpaceChangefeedClosed) {
		t.Fatalf("append after completion error = %v", err)
	}
}
