package hatPipeline

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestConnectorTransactionJournalLifecycleAndRetry(t *testing.T) {
	journal, err := NewConnectorTransactionJournal(context.Background(), ConnectorTransactionJournalOptions{Capacity: 4})
	if err != nil {
		t.Fatal(err)
	}
	intent := ConnectorTransactionIntent{
		ID:          "tx-1",
		ConnectorID: "orders",
		Generation:  7,
		Offset:      []byte("offset-1"),
		Frontier:    []byte("frontier-1"),
	}
	transaction, err := journal.Begin(context.Background(), intent)
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	if transaction.State != ConnectorTransactionPending || transaction.Attempt != 1 || transaction.Sequence != 1 {
		t.Fatalf("begin transaction = %+v", transaction)
	}
	duplicate, err := journal.Begin(context.Background(), intent)
	if err != nil || !reflect.DeepEqual(duplicate, transaction) {
		t.Fatalf("idempotent Begin() = %+v, %v, want %+v", duplicate, err, transaction)
	}
	failed, err := journal.Fail(context.Background(), "tx-1", 7, "temporary source failure")
	if err != nil {
		t.Fatalf("Fail() error = %v", err)
	}
	if failed.State != ConnectorTransactionFailed || failed.LastError != "temporary source failure" {
		t.Fatalf("failed transaction = %+v", failed)
	}
	retried, err := journal.Retry(context.Background(), "tx-1", 7)
	if err != nil {
		t.Fatalf("Retry() error = %v", err)
	}
	if retried.State != ConnectorTransactionPending || retried.Attempt != 2 || retried.LastError != "" {
		t.Fatalf("retried transaction = %+v", retried)
	}
	committed, err := journal.Commit(context.Background(), "tx-1", 7)
	if err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if committed.State != ConnectorTransactionCommitted || committed.Attempt != 2 {
		t.Fatalf("committed transaction = %+v", committed)
	}
	if got, ok := journal.Get("tx-1"); !ok || !reflect.DeepEqual(got, committed) {
		t.Fatalf("Get() = %+v/%t, want %+v/true", got, ok, committed)
	}
}

func TestConnectorTransactionJournalPersistenceAndCorruption(t *testing.T) {
	store := &connectorTransactionMemoryStore{}
	journal, err := NewConnectorTransactionJournal(context.Background(), ConnectorTransactionJournalOptions{Capacity: 4, Store: store})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Begin(context.Background(), ConnectorTransactionIntent{ID: "tx-1", ConnectorID: "orders", Generation: 1, Offset: []byte("o"), Frontier: []byte("f")}); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Fail(context.Background(), "tx-1", 1, "retry"); err != nil {
		t.Fatal(err)
	}
	restored, err := NewConnectorTransactionJournal(context.Background(), ConnectorTransactionJournalOptions{Capacity: 4, Store: store})
	if err != nil {
		t.Fatalf("restore error = %v", err)
	}
	got, ok := restored.Get("tx-1")
	if !ok || got.State != ConnectorTransactionFailed || got.Attempt != 1 {
		t.Fatalf("restored transaction = %+v/%t", got, ok)
	}
	snapshot := restored.Snapshot()
	payload, err := EncodeConnectorTransactionSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeConnectorTransactionSnapshot(payload)
	if err != nil || len(decoded.Transactions) != 1 || !reflect.DeepEqual(decoded.Transactions[0], got) {
		t.Fatalf("round trip = %+v, %v", decoded, err)
	}
	payload[len(payload)-1] ^= 1
	if _, err := DecodeConnectorTransactionSnapshot(payload); !errors.Is(err, ErrConnectorTransactionCorrupt) {
		t.Fatalf("corrupt decode error = %v, want ErrConnectorTransactionCorrupt", err)
	}
}

func TestConnectorTransactionJournalFencesGenerationAndBoundsRetention(t *testing.T) {
	journal, err := NewConnectorTransactionJournal(context.Background(), ConnectorTransactionJournalOptions{Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Begin(context.Background(), ConnectorTransactionIntent{ID: "tx-1", ConnectorID: "orders", Generation: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Commit(context.Background(), "tx-1", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Begin(context.Background(), ConnectorTransactionIntent{ID: "tx-1", ConnectorID: "orders", Generation: 2}); !errors.Is(err, ErrConnectorTransactionIntentMismatch) {
		t.Fatalf("generation reuse error = %v, want mismatch", err)
	}
	if _, err := journal.Begin(context.Background(), ConnectorTransactionIntent{ID: "tx-2", ConnectorID: "orders", Generation: 1}); err != nil {
		t.Fatalf("retained terminal Begin() error = %v", err)
	}
	if snapshot := journal.Snapshot(); snapshot.Dropped == 0 || len(snapshot.Transactions) != 1 || snapshot.Transactions[0].ID != "tx-2" {
		t.Fatalf("retention snapshot = %+v", snapshot)
	}
	pending, err := NewConnectorTransactionJournal(context.Background(), ConnectorTransactionJournalOptions{Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pending.Begin(context.Background(), ConnectorTransactionIntent{ID: "tx-pending", ConnectorID: "orders", Generation: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.Begin(context.Background(), ConnectorTransactionIntent{ID: "tx-other", ConnectorID: "orders", Generation: 1}); !errors.Is(err, ErrConnectorTransactionJournalFull) {
		t.Fatalf("pending full error = %v, want full", err)
	}
}

func TestConnectorTransactionJournalRollbackAndValidation(t *testing.T) {
	storeErr := errors.New("journal save failed")
	store := &connectorTransactionMemoryStore{saveErr: storeErr}
	journal, err := NewConnectorTransactionJournal(context.Background(), ConnectorTransactionJournalOptions{Capacity: 2, Store: store})
	if err != nil {
		t.Fatal(err)
	}
	intent := ConnectorTransactionIntent{ID: "tx-1", ConnectorID: "orders", Generation: 1}
	if _, err := journal.Begin(context.Background(), intent); !errors.Is(err, storeErr) {
		t.Fatalf("save error = %v, want %v", err, storeErr)
	}
	if _, ok := journal.Get("tx-1"); ok || journal.Snapshot().Revision != 0 {
		t.Fatal("failed begin left journal state")
	}
	for _, invalid := range []ConnectorTransactionIntent{
		{ConnectorID: "orders", Generation: 1},
		{ID: strings.Repeat("x", MaxConnectorTransactionIDBytes+1), ConnectorID: "orders", Generation: 1},
		{ID: "tx", ConnectorID: "orders"},
	} {
		if _, err := journal.Begin(context.Background(), invalid); !errors.Is(err, ErrConnectorTransactionInvalid) {
			t.Fatalf("invalid intent %#v error = %v, want invalid", invalid, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := journal.Begin(ctx, intent); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Begin() error = %v, want context.Canceled", err)
	}
}

type connectorTransactionMemoryStore struct {
	data    []byte
	saveErr error
}

func (store *connectorTransactionMemoryStore) Load(context.Context) ([]byte, error) {
	return append([]byte(nil), store.data...), nil
}

func (store *connectorTransactionMemoryStore) Save(_ context.Context, data []byte) error {
	if store.saveErr != nil {
		return store.saveErr
	}
	store.data = append(store.data[:0], data...)
	return nil
}
