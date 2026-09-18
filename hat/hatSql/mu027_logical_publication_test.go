package hatSql

import (
	"context"
	"errors"
	"testing"
)

func TestSQLPublicationReplaysVersionsAndAcksCheckpoints(t *testing.T) {
	publication, err := NewSQLPublication("orders", []string{"id", "status"}, SQLPublicationOptions{
		MaxHistoryBatches: 4,
		MaxPendingBatches: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	firstRow := Row{"id": int64(1), "status": "new"}
	if err := publication.Append(SQLPublicationBatch{
		Revision: 1,
		Frontier: 10,
		Deltas:   []SQLPublicationDelta{{Row: firstRow, Diff: 1}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := publication.Append(SQLPublicationBatch{
		Revision: 2,
		Frontier: 11,
		Deltas:   []SQLPublicationDelta{{Row: Row{"id": int64(1), "status": "paid"}, Diff: 1}},
	}); err != nil {
		t.Fatal(err)
	}
	firstRow["status"] = "mutated after append"

	subscription, err := publication.Subscribe(context.Background(), SQLPublicationCheckpoint{})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	first := <-subscription.Updates()
	second := <-subscription.Updates()
	if first.Revision != 1 || first.Frontier != 10 || first.Deltas[0].Row["status"] != "new" {
		t.Fatalf("first batch = %#v", first)
	}
	if second.Revision != 2 || second.Deltas[0].Row["status"] != "paid" {
		t.Fatalf("second batch = %#v", second)
	}
	if err := subscription.Ack(SQLPublicationCheckpoint{Revision: 1, Frontier: 10}); err != nil {
		t.Fatal(err)
	}
	if got := subscription.Checkpoint(); got != (SQLPublicationCheckpoint{Revision: 1, Frontier: 10}) {
		t.Fatalf("Checkpoint() = %#v", got)
	}

	replay, err := publication.Subscribe(context.Background(), subscription.Checkpoint())
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Close()
	if got := <-replay.Updates(); got.Revision != 2 {
		t.Fatalf("replayed revision = %d, want 2", got.Revision)
	}
}

func TestSQLPublicationValidatesSequenceSchemaAndBounds(t *testing.T) {
	publication, err := NewSQLPublication("orders", []string{"id"}, SQLPublicationOptions{
		MaxHistoryBatches: 1,
		MaxBatchDeltas:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	valid := SQLPublicationBatch{
		Revision: 1,
		Frontier: 10,
		Deltas:   []SQLPublicationDelta{{Row: Row{"id": int64(1)}, Diff: 1}},
	}
	if err := publication.Append(valid); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []SQLPublicationBatch{
		{Revision: 3, Frontier: 11},
		{Revision: 2, Frontier: 9},
		{Revision: 2, Frontier: 11, Columns: []string{"other"}},
		{Revision: 2, Frontier: 11, Deltas: []SQLPublicationDelta{{Row: Row{"id": int64(2)}, Diff: 0}}},
		{Revision: 2, Frontier: 11, Deltas: []SQLPublicationDelta{
			{Row: Row{"id": int64(2)}, Diff: 1},
			{Row: Row{"id": int64(3)}, Diff: 1},
		}},
	} {
		if err := publication.Append(invalid); !errors.Is(err, ErrSQLPublicationInvalid) && !errors.Is(err, ErrSQLPublicationSequence) && !errors.Is(err, ErrSQLPublicationLimit) {
			t.Fatalf("Append(%#v) error = %v, want publication validation error", invalid, err)
		}
	}
	if snapshot := publication.Snapshot(); snapshot.LatestRevision != 1 || snapshot.HistoryBatches != 1 {
		t.Fatalf("Snapshot() after invalid appends = %#v", snapshot)
	}
}

func TestSQLPublicationBackpressureAndCompletion(t *testing.T) {
	publication, err := NewSQLPublication("orders", []string{"id"}, SQLPublicationOptions{
		MaxPendingBatches: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := publication.Subscribe(context.Background(), SQLPublicationCheckpoint{})
	if err != nil {
		t.Fatal(err)
	}
	appendBatch := func(revision uint64) error {
		return publication.Append(SQLPublicationBatch{
			Revision: revision,
			Frontier: revision,
			Deltas:   []SQLPublicationDelta{{Row: Row{"id": int64(revision)}, Diff: 1}},
		})
	}
	if err := appendBatch(1); err != nil {
		t.Fatal(err)
	}
	if err := appendBatch(2); !errors.Is(err, ErrSQLPublicationBackpressure) {
		t.Fatalf("second append error = %v, want backpressure", err)
	}
	if err := subscription.Err(); !errors.Is(err, ErrSQLPublicationBackpressure) {
		t.Fatalf("subscription Err() = %v, want backpressure", err)
	}
	subscription.Close()

	completed, err := NewSQLPublication("completed", []string{"id"}, SQLPublicationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	finished, err := completed.Subscribe(context.Background(), SQLPublicationCheckpoint{})
	if err != nil {
		t.Fatal(err)
	}
	if err := completed.Append(SQLPublicationBatch{Revision: 1, Frontier: 1, Complete: true}); err != nil {
		t.Fatal(err)
	}
	if batch := <-finished.Updates(); !batch.Complete {
		t.Fatalf("completion batch = %#v", batch)
	}
	if err := finished.Err(); err != nil {
		t.Fatalf("completed subscription Err() = %v", err)
	}
}

func TestSQLPublicationCheckpointBinaryRoundTrip(t *testing.T) {
	want := SQLPublicationCheckpoint{Revision: 42, Frontier: 99}
	encoded, err := want.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var got SQLPublicationCheckpoint
	if err := got.UnmarshalBinary(encoded); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("decoded checkpoint = %#v, want %#v", got, want)
	}
	encoded[len(encoded)-1] ^= 1
	if err := got.UnmarshalBinary(encoded); !errors.Is(err, ErrSQLPublicationInvalid) {
		t.Fatalf("corrupt checkpoint error = %v, want invalid", err)
	}
}

func TestSQLPublicationCheckpointExpiryAndCompletionAck(t *testing.T) {
	publication, err := NewSQLPublication("orders", []string{"id"}, SQLPublicationOptions{
		MaxHistoryBatches: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	for revision := uint64(1); revision <= 3; revision++ {
		if err := publication.Append(SQLPublicationBatch{
			Revision: revision,
			Frontier: revision,
			Deltas:   []SQLPublicationDelta{{Row: Row{"id": int64(revision)}, Diff: 1}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := publication.Subscribe(context.Background(), SQLPublicationCheckpoint{Revision: 1, Frontier: 1}); !errors.Is(err, ErrSQLPublicationCheckpointExpired) {
		t.Fatalf("expired Subscribe() error = %v, want checkpoint expired", err)
	}

	completed, err := NewSQLPublication("completed", []string{"id"}, SQLPublicationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := completed.Subscribe(context.Background(), SQLPublicationCheckpoint{})
	if err != nil {
		t.Fatal(err)
	}
	if err := completed.Append(SQLPublicationBatch{Revision: 1, Frontier: 1, Complete: true}); err != nil {
		t.Fatal(err)
	}
	batch := <-subscription.Updates()
	if err := subscription.Ack(SQLPublicationCheckpoint{Revision: batch.Revision, Frontier: batch.Frontier}); err != nil {
		t.Fatalf("completion Ack() error = %v", err)
	}
	select {
	case <-subscription.Done():
	default:
		t.Fatal("completed subscription Done() is not closed")
	}
}

func TestSQLPublicationRejectsInvalidOptions(t *testing.T) {
	for _, options := range []SQLPublicationOptions{
		{MaxHistoryBatches: -1},
		{MaxBatchDeltas: -1},
		{MaxPendingBatches: -1},
		{MaxSubscribers: -1},
		{MaxColumns: -1},
		{MaxRowColumns: -1},
	} {
		if _, err := NewSQLPublication("orders", []string{"id"}, options); !errors.Is(err, ErrSQLPublicationInvalid) {
			t.Fatalf("options %#v error = %v, want invalid", options, err)
		}
	}
}
