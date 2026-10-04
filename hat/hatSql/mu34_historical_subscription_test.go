package hatSql

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSQLPublicationHistoricalReplayCancellationPreservesCheckpoint(t *testing.T) {
	publication, err := NewSQLPublication("orders", []string{"id"}, SQLPublicationOptions{
		MaxHistoryBatches: 8,
		MaxPendingBatches: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	subscription, err := publication.Subscribe(ctx, SQLPublicationCheckpoint{})
	if err != nil {
		t.Fatal(err)
	}
	first := SQLPublicationBatch{
		Revision: 1,
		Frontier: 1,
		Deltas:   []SQLPublicationDelta{{Row: Row{"id": int64(1)}, Diff: 1}},
	}
	if err := publication.Append(first); err != nil {
		t.Fatal(err)
	}
	select {
	case batch := <-subscription.Updates():
		if batch.Revision != first.Revision {
			t.Fatalf("revision = %d, want %d", batch.Revision, first.Revision)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for replay batch")
	}
	checkpoint := SQLPublicationCheckpoint{Revision: 1, Frontier: 1}
	if err := subscription.Ack(checkpoint); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-subscription.Done():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for canceled subscription")
	}
	if !errors.Is(subscription.Err(), context.Canceled) {
		t.Fatalf("subscription error = %v, want context.Canceled", subscription.Err())
	}

	encoded, err := subscription.Checkpoint().MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var persisted SQLPublicationCheckpoint
	if err := persisted.UnmarshalBinary(encoded); err != nil {
		t.Fatal(err)
	}
	if persisted != checkpoint {
		t.Fatalf("persisted checkpoint = %+v, want %+v", persisted, checkpoint)
	}

	resumed, err := publication.Subscribe(context.Background(), persisted)
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Close()
	second := SQLPublicationBatch{
		Revision: 2,
		Frontier: 2,
		Deltas:   []SQLPublicationDelta{{Row: Row{"id": int64(2)}, Diff: 1}},
	}
	if err := publication.Append(second); err != nil {
		t.Fatal(err)
	}
	select {
	case batch := <-resumed.Updates():
		if batch.Revision != second.Revision {
			t.Fatalf("resumed revision = %d, want %d", batch.Revision, second.Revision)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for resumed batch")
	}
}

func TestSQLPublicationSubscriptionCancelIsObservable(t *testing.T) {
	publication, err := NewSQLPublication("orders", []string{"id"}, SQLPublicationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := publication.Subscribe(context.Background(), SQLPublicationCheckpoint{})
	if err != nil {
		t.Fatal(err)
	}
	subscription.Cancel()
	select {
	case <-subscription.Done():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for canceled subscription")
	}
	if !errors.Is(subscription.Err(), ErrSQLPublicationCanceled) {
		t.Fatalf("subscription error = %v, want ErrSQLPublicationCanceled", subscription.Err())
	}
	if snapshot := publication.Snapshot(); snapshot.Subscribers != 0 {
		t.Fatalf("subscribers = %d, want 0", snapshot.Subscribers)
	}
}
