package hatSql

import (
	"context"
	"errors"
	"sync"
	"testing"
)

type mu034SubscriptionStore struct {
	mu      sync.Mutex
	records map[string]SQLPublicationSubscriptionRecord
	err     error
}

func (store *mu034SubscriptionStore) Load(_ context.Context, publication, subscriptionID string) (SQLPublicationSubscriptionRecord, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.err != nil {
		return SQLPublicationSubscriptionRecord{}, false, store.err
	}
	record, ok := store.records[publication+"/"+subscriptionID]
	return record, ok, nil
}

func (store *mu034SubscriptionStore) Commit(_ context.Context, record SQLPublicationSubscriptionRecord) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.err != nil {
		return store.err
	}
	if store.records == nil {
		store.records = make(map[string]SQLPublicationSubscriptionRecord)
	}
	store.records[record.PublicationName+"/"+record.SubscriptionID] = record
	return nil
}

func TestM034DurableHistoricalSubscriptionResumesFromAckAfterClose(t *testing.T) {
	publication, err := NewSQLPublication("orders", []string{"id"}, SQLPublicationOptions{MaxHistoryBatches: 8})
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
	store := &mu034SubscriptionStore{}
	options := SQLPublicationDurableSubscriptionOptions{SubscriptionID: "orders-worker", Store: store}
	first, err := publication.SubscribeDurable(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	firstBatch := <-first.Updates()
	if firstBatch.Revision != 1 {
		t.Fatalf("first replay revision = %d, want 1", firstBatch.Revision)
	}
	if err := first.Ack(SQLPublicationCheckpoint{Revision: 1, Frontier: 1}); err != nil {
		t.Fatal(err)
	}
	first.Close()

	resumed, err := publication.SubscribeDurable(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Close()
	for wantRevision := uint64(2); wantRevision <= 3; wantRevision++ {
		batch := <-resumed.Updates()
		if batch.Revision != wantRevision {
			t.Fatalf("resumed revision = %d, want %d", batch.Revision, wantRevision)
		}
		if err := resumed.Ack(SQLPublicationCheckpoint{Revision: batch.Revision, Frontier: batch.Frontier}); err != nil {
			t.Fatal(err)
		}
	}
	store.mu.Lock()
	record := store.records["orders/orders-worker"]
	store.mu.Unlock()
	if record.Checkpoint != (SQLPublicationCheckpoint{Revision: 3, Frontier: 3}) || record.State != SQLPublicationSubscriptionStateActive {
		t.Fatalf("durable record = %#v, want active revision 3", record)
	}
}

func TestM034DurableHistoricalSubscriptionCancellationIsTerminal(t *testing.T) {
	publication, err := NewSQLPublication("orders", []string{"id"}, SQLPublicationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := publication.Append(SQLPublicationBatch{Revision: 1, Frontier: 1, Deltas: []SQLPublicationDelta{{Row: Row{"id": int64(1)}, Diff: 1}}}); err != nil {
		t.Fatal(err)
	}
	store := &mu034SubscriptionStore{}
	options := SQLPublicationDurableSubscriptionOptions{SubscriptionID: "orders-worker", Store: store}
	subscription, err := publication.SubscribeDurable(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	if err := subscription.Cancel(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-subscription.Done():
	default:
		t.Fatal("cancelled subscription remains active")
	}
	store.mu.Lock()
	record := store.records["orders/orders-worker"]
	store.mu.Unlock()
	if record.State != SQLPublicationSubscriptionStateCancelled {
		t.Fatalf("cancelled record state = %q", record.State)
	}
	if _, err := publication.SubscribeDurable(context.Background(), options); !errors.Is(err, ErrSQLPublicationSubscriptionCancelled) {
		t.Fatalf("resubscribe error = %v, want cancelled", err)
	}
}

func TestM034DurableHistoricalSubscriptionDoesNotAdvanceOnStoreFailure(t *testing.T) {
	publication, err := NewSQLPublication("orders", []string{"id"}, SQLPublicationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := publication.Append(SQLPublicationBatch{Revision: 1, Frontier: 1, Deltas: []SQLPublicationDelta{{Row: Row{"id": int64(1)}, Diff: 1}}}); err != nil {
		t.Fatal(err)
	}
	store := &mu034SubscriptionStore{err: errors.New("store unavailable")}
	options := SQLPublicationDurableSubscriptionOptions{SubscriptionID: "orders-worker", Store: store}
	if _, err := publication.SubscribeDurable(context.Background(), options); err == nil {
		t.Fatal("SubscribeDurable() succeeded with unavailable store")
	}

	store.err = nil
	subscription, err := publication.SubscribeDurable(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	batch := <-subscription.Updates()
	store.err = errors.New("store unavailable")
	if err := subscription.Ack(SQLPublicationCheckpoint{Revision: batch.Revision, Frontier: batch.Frontier}); err == nil {
		t.Fatal("Ack() succeeded with unavailable store")
	}
	if got := subscription.Checkpoint(); got != (SQLPublicationCheckpoint{}) {
		t.Fatalf("checkpoint advanced after failed store commit = %#v", got)
	}
	if err := subscription.Cancel(context.Background()); err == nil {
		t.Fatal("Cancel() succeeded with unavailable store")
	}
	select {
	case <-subscription.Done():
		t.Fatal("failed cancellation closed subscription")
	default:
	}
}

func TestM034DurableHistoricalSubscriptionCanCancelAfterResumableClose(t *testing.T) {
	publication, err := NewSQLPublication("orders", []string{"id"}, SQLPublicationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := &mu034SubscriptionStore{}
	subscription, err := publication.SubscribeDurable(context.Background(), SQLPublicationDurableSubscriptionOptions{
		SubscriptionID: "orders-worker",
		Store:          store,
	})
	if err != nil {
		t.Fatal(err)
	}
	subscription.Close()
	if err := subscription.Cancel(context.Background()); err != nil {
		t.Fatalf("Cancel() after Close() error = %v", err)
	}
	store.mu.Lock()
	record := store.records["orders/orders-worker"]
	store.mu.Unlock()
	if record.State != SQLPublicationSubscriptionStateCancelled {
		t.Fatalf("record state after close/cancel = %q", record.State)
	}
}
