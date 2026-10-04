package hatSql

import (
	"context"
	"errors"
	"testing"
)

type mu034CheckpointStore struct {
	checkpoint SQLPublicationCheckpoint
	found      bool
	failSave   bool
}

func (store *mu034CheckpointStore) Load(context.Context, string, string) (SQLPublicationCheckpoint, bool, error) {
	return store.checkpoint, store.found, nil
}

func (store *mu034CheckpointStore) Save(_ context.Context, _, _ string, checkpoint SQLPublicationCheckpoint) error {
	if store.failSave {
		return errors.New("checkpoint save failed")
	}
	store.checkpoint = checkpoint
	store.found = true
	return nil
}

func TestSQLPublicationDurableSubscriptionResumesAfterCancellation(t *testing.T) {
	publication, err := NewSQLPublication("orders", []string{"id"}, SQLPublicationOptions{MaxHistoryBatches: 8})
	if err != nil {
		t.Fatal(err)
	}
	appendSQLPublicationBatch(t, publication, 1, 1)
	appendSQLPublicationBatch(t, publication, 2, 2)

	store := &mu034CheckpointStore{}
	subscription, err := publication.SubscribeDurable(context.Background(), "consumer-a", store)
	if err != nil {
		t.Fatal(err)
	}
	if got := <-subscription.Updates(); got.Revision != 1 {
		t.Fatalf("first replay revision = %d, want 1", got.Revision)
	}
	if got := <-subscription.Updates(); got.Revision != 2 {
		t.Fatalf("second replay revision = %d, want 2", got.Revision)
	}
	if err := subscription.AckWithStore(context.Background(), store, SQLPublicationCheckpoint{Revision: 1, Frontier: 1}); err != nil {
		t.Fatal(err)
	}
	appendSQLPublicationBatch(t, publication, 3, 3)
	if err := subscription.Cancel(context.Background(), store); err != nil {
		t.Fatal(err)
	}

	resumed, err := publication.SubscribeDurable(context.Background(), "consumer-a", store)
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Close()
	if got := <-resumed.Updates(); got.Revision != 2 {
		t.Fatalf("resumed first revision = %d, want 2", got.Revision)
	}
	if got := <-resumed.Updates(); got.Revision != 3 {
		t.Fatalf("resumed second revision = %d, want 3", got.Revision)
	}
}

func TestSQLPublicationDurableCancelKeepsSubscriptionOpenOnSaveFailure(t *testing.T) {
	publication, err := NewSQLPublication("orders", []string{"id"}, SQLPublicationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	appendSQLPublicationBatch(t, publication, 1, 1)
	store := &mu034CheckpointStore{failSave: true}
	subscription, err := publication.SubscribeDurable(context.Background(), "consumer-a", store)
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	<-subscription.Updates()
	if err := subscription.Cancel(context.Background(), store); err == nil {
		t.Fatal("Cancel should report checkpoint persistence failure")
	}
	select {
	case <-subscription.Done():
		t.Fatal("subscription closed after failed checkpoint persistence")
	default:
	}
}

func TestSQLPublicationCheckpointStoreRequiresDurableSubscription(t *testing.T) {
	publication, err := NewSQLPublication("orders", []string{"id"}, SQLPublicationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := publication.Subscribe(context.Background(), SQLPublicationCheckpoint{})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	store := &mu034CheckpointStore{}
	if err := subscription.AckWithStore(context.Background(), store, SQLPublicationCheckpoint{}); !errors.Is(err, ErrSQLPublicationSubscriptionNotDurable) {
		t.Fatalf("AckWithStore error = %v, want not-durable error", err)
	}
	if err := subscription.Cancel(context.Background(), store); !errors.Is(err, ErrSQLPublicationSubscriptionNotDurable) {
		t.Fatalf("Cancel error = %v, want not-durable error", err)
	}
}

func appendSQLPublicationBatch(t *testing.T, publication *SQLPublication, revision, frontier uint64) {
	t.Helper()
	if err := publication.Append(SQLPublicationBatch{
		Revision: revision,
		Frontier: frontier,
		Deltas:   []SQLPublicationDelta{{Row: Row{"id": revision}, Diff: 1}},
	}); err != nil {
		t.Fatal(err)
	}
}
