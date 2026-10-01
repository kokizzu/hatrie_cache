package hatCache

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

type mU34SubscriptionCheckpointStore struct {
	mu      sync.Mutex
	value   uint64
	saveErr error
}

func (store *mU34SubscriptionCheckpointStore) Load(context.Context) (uint64, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.value, nil
}

func (store *mU34SubscriptionCheckpointStore) Save(_ context.Context, sequence uint64) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.saveErr != nil {
		return store.saveErr
	}
	store.value = sequence
	return nil
}

func (store *mU34SubscriptionCheckpointStore) setSaveError(err error) {
	store.mu.Lock()
	store.saveErr = err
	store.mu.Unlock()
}

func (store *mU34SubscriptionCheckpointStore) sequence() uint64 {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.value
}

func TestCommandJournalSubscriptionCheckpointResumesUnacknowledgedRecords(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	for _, key := range []string{"one", "two", "three"} {
		appendJournalSubscriptionTestCommand(t, journal, trie, key)
	}

	store := &mU34SubscriptionCheckpointStore{}
	first, err := journal.SubscribeWithCheckpoint(context.Background(), store, CommandJournalSubscribeOptions{
		ReplayLimit: 10,
		Buffer:      3,
	})
	if err != nil {
		t.Fatalf("SubscribeWithCheckpoint() error = %v", err)
	}
	firstRecord := receiveJournalSubscriptionRecord(t, first)
	if firstRecord.Sequence != 1 {
		t.Fatalf("first replay sequence = %d, want 1", firstRecord.Sequence)
	}
	if err := first.Acknowledge(context.Background(), firstRecord.Sequence); err != nil {
		t.Fatalf("Acknowledge(1) error = %v", err)
	}
	secondRecord := receiveJournalSubscriptionRecord(t, first)
	if secondRecord.Sequence != 2 {
		t.Fatalf("second replay sequence = %d, want 2", secondRecord.Sequence)
	}
	first.Close()
	if got := store.sequence(); got != 1 {
		t.Fatalf("checkpoint after cancellation = %d, want 1", got)
	}

	second, err := journal.SubscribeWithCheckpoint(context.Background(), store, CommandJournalSubscribeOptions{
		ReplayLimit: 10,
		Buffer:      3,
	})
	if err != nil {
		t.Fatalf("restart SubscribeWithCheckpoint() error = %v", err)
	}
	for wantSequence := uint64(2); wantSequence <= 3; wantSequence++ {
		record := receiveJournalSubscriptionRecord(t, second)
		if record.Sequence != wantSequence {
			t.Fatalf("restart replay sequence = %d, want %d", record.Sequence, wantSequence)
		}
		if err := second.Acknowledge(context.Background(), record.Sequence); err != nil {
			t.Fatalf("restart Acknowledge(%d) error = %v", record.Sequence, err)
		}
	}
	second.Close()
	if got := store.sequence(); got != 3 {
		t.Fatalf("final checkpoint = %d, want 3", got)
	}
}

func TestCommandJournalSubscriptionCheckpointValidatesAcknowledgements(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	appendJournalSubscriptionTestCommand(t, journal, trie, "one")
	store := &mU34SubscriptionCheckpointStore{}
	store.value = 2
	if _, err := journal.SubscribeWithCheckpoint(context.Background(), store, CommandJournalSubscribeOptions{}); !errors.Is(err, ErrCommandJournalSubscriptionCheckpointAhead) {
		t.Fatalf("ahead checkpoint error = %v, want ErrCommandJournalSubscriptionCheckpointAhead", err)
	}
	store.value = 0
	subscription, err := journal.SubscribeWithCheckpoint(context.Background(), store, CommandJournalSubscribeOptions{
		ReplayLimit: 1,
		Buffer:      1,
	})
	if err != nil {
		t.Fatalf("SubscribeWithCheckpoint() error = %v", err)
	}
	defer subscription.Close()

	if err := subscription.Acknowledge(context.Background(), 1); !errors.Is(err, ErrCommandJournalSubscriptionCheckpointSequence) {
		t.Fatalf("Acknowledge(undelivered) error = %v, want sequence validation", err)
	}
	record := receiveJournalSubscriptionRecord(t, subscription)
	if err := subscription.Acknowledge(context.Background(), record.Sequence); err != nil {
		t.Fatalf("Acknowledge(delivered) error = %v", err)
	}
	if err := subscription.Acknowledge(context.Background(), record.Sequence); err != nil {
		t.Fatalf("idempotent Acknowledge() error = %v", err)
	}
	if got := subscription.Checkpoint(); got != record.Sequence {
		t.Fatalf("subscription checkpoint = %d, want %d", got, record.Sequence)
	}
}

func TestCommandJournalSubscriptionCheckpointSaveFailureIsRetryable(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	appendJournalSubscriptionTestCommand(t, journal, trie, "one")
	store := &mU34SubscriptionCheckpointStore{}
	subscription, err := journal.SubscribeWithCheckpoint(context.Background(), store, CommandJournalSubscribeOptions{
		ReplayLimit: 1,
		Buffer:      1,
	})
	if err != nil {
		t.Fatalf("SubscribeWithCheckpoint() error = %v", err)
	}
	defer subscription.Close()
	record := receiveJournalSubscriptionRecord(t, subscription)
	saveErr := errors.New("checkpoint unavailable")
	store.setSaveError(saveErr)
	if err := subscription.Acknowledge(context.Background(), record.Sequence); !errors.Is(err, saveErr) {
		t.Fatalf("Acknowledge() error = %v, want %v", err, saveErr)
	}
	if got := subscription.Checkpoint(); got != 0 {
		t.Fatalf("checkpoint after failed save = %d, want 0", got)
	}
	store.setSaveError(nil)
	if err := subscription.Acknowledge(context.Background(), record.Sequence); err != nil {
		t.Fatalf("retry Acknowledge() error = %v", err)
	}
}

func TestFileCommandJournalCheckpointStoreRecoversAndRejectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "checkpoint.bin")
	store, err := NewFileCommandJournalCheckpointStore(path)
	if err != nil {
		t.Fatalf("NewFileCommandJournalCheckpointStore() error = %v", err)
	}
	if got, err := store.Load(context.Background()); err != nil || got != 0 {
		t.Fatalf("initial Load() = %d, %v, want zero checkpoint", got, err)
	}
	if err := store.Save(context.Background(), 42); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	reopened, err := NewFileCommandJournalCheckpointStore(path)
	if err != nil {
		t.Fatalf("reopen NewFileCommandJournalCheckpointStore() error = %v", err)
	}
	if got, err := reopened.Load(context.Background()); err != nil || got != 42 {
		t.Fatalf("reopened Load() = %d, %v, want 42", got, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("checkpoint permissions = %o, want 600", got)
	}
	if err := os.WriteFile(path, []byte("corrupt"), 0o600); err != nil {
		t.Fatalf("WriteFile(corrupt) error = %v", err)
	}
	if _, err := reopened.Load(context.Background()); !errors.Is(err, ErrInvalidCommandJournalCheckpoint) {
		t.Fatalf("corrupt Load() error = %v, want ErrInvalidCommandJournalCheckpoint", err)
	}
}
