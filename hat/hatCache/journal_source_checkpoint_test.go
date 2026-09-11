package hatCache

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

var errMZ013SourceCheckpointStore = errors.New("mz013 source checkpoint store failure")

type mz013SourceCheckpointStore struct {
	mu          sync.Mutex
	checkpoints map[string]CommandJournalSourceCheckpoint
	saveErr     error
	saveCount   int
}

func (store *mz013SourceCheckpointStore) Load(_ context.Context, sourceID string) (CommandJournalSourceCheckpoint, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	checkpoint := store.checkpoints[sourceID]
	checkpoint.Offset = append([]byte(nil), checkpoint.Offset...)
	return checkpoint, nil
}

func (store *mz013SourceCheckpointStore) Save(_ context.Context, sourceID string, checkpoint CommandJournalSourceCheckpoint) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.saveCount++
	if store.saveErr != nil {
		return store.saveErr
	}
	if store.checkpoints == nil {
		store.checkpoints = make(map[string]CommandJournalSourceCheckpoint)
	}
	checkpoint.Offset = append([]byte(nil), checkpoint.Offset...)
	store.checkpoints[sourceID] = checkpoint
	return nil
}

func (store *mz013SourceCheckpointStore) checkpoint(sourceID string) CommandJournalSourceCheckpoint {
	store.mu.Lock()
	defer store.mu.Unlock()
	checkpoint := store.checkpoints[sourceID]
	checkpoint.Offset = append([]byte(nil), checkpoint.Offset...)
	return checkpoint
}

func (store *mz013SourceCheckpointStore) saves() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.saveCount
}

func TestCommandJournalSourceCheckpointCommitBindsOffsetToAppliedJournalSequence(t *testing.T) {
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	trie := newTestTrie(t)
	defer trie.Destroy()
	if response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "INC", Key: "source-counter"}); !response.OK {
		t.Fatalf("journal.ExecuteCommand() response = %#v", response)
	}

	store := &mz013SourceCheckpointStore{}
	coordinator, err := NewCommandJournalSourceCheckpointCoordinator(journal, store)
	if err != nil {
		t.Fatalf("NewCommandJournalSourceCheckpointCoordinator() error = %v", err)
	}
	offset := []byte{0, 1, 255}
	checkpoint, err := coordinator.Commit(context.Background(), "orders-eu", offset)
	if err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if checkpoint.JournalSequence != 1 || !bytes.Equal(checkpoint.Offset, offset) {
		t.Fatalf("checkpoint = %#v, want sequence 1 and binary offset", checkpoint)
	}
	stored := store.checkpoint("orders-eu")
	if stored.JournalSequence != 1 || !bytes.Equal(stored.Offset, offset) {
		t.Fatalf("stored checkpoint = %#v, want sequence 1 and binary offset", stored)
	}
	offset[0] = 9
	if stored.Offset[0] != 0 {
		t.Fatal("Commit() retained caller-owned offset bytes")
	}
}

func TestCommandJournalSourceCheckpointLoadCopiesOffsetAndRejectsAheadSequence(t *testing.T) {
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	store := &mz013SourceCheckpointStore{checkpoints: map[string]CommandJournalSourceCheckpoint{
		"orders-eu": {Offset: []byte{1, 2, 3}, JournalSequence: 0},
	}}
	coordinator, err := NewCommandJournalSourceCheckpointCoordinator(journal, store)
	if err != nil {
		t.Fatalf("NewCommandJournalSourceCheckpointCoordinator() error = %v", err)
	}
	checkpoint, err := coordinator.Load(context.Background(), "orders-eu")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	checkpoint.Offset[0] = 9
	loadedAgain, err := coordinator.Load(context.Background(), "orders-eu")
	if err != nil {
		t.Fatalf("Load() second call error = %v", err)
	}
	if loadedAgain.Offset[0] != 1 {
		t.Fatal("Load() returned store-owned offset bytes")
	}

	store.checkpoints["orders-eu"] = CommandJournalSourceCheckpoint{JournalSequence: 1}
	if _, err := coordinator.Load(context.Background(), "orders-eu"); !errors.Is(err, ErrCommandJournalSourceCheckpointAhead) {
		t.Fatalf("Load() ahead sequence error = %v, want ErrCommandJournalSourceCheckpointAhead", err)
	}
}

func TestCommandJournalSourceCheckpointCommitFailureDoesNotAdvanceStore(t *testing.T) {
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	trie := newTestTrie(t)
	defer trie.Destroy()
	if response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "INC", Key: "source-counter"}); !response.OK {
		t.Fatalf("journal.ExecuteCommand() response = %#v", response)
	}
	store := &mz013SourceCheckpointStore{saveErr: errMZ013SourceCheckpointStore}
	coordinator, err := NewCommandJournalSourceCheckpointCoordinator(journal, store)
	if err != nil {
		t.Fatalf("NewCommandJournalSourceCheckpointCoordinator() error = %v", err)
	}
	if _, err := coordinator.Commit(context.Background(), "orders-eu", []byte("offset-1")); !errors.Is(err, errMZ013SourceCheckpointStore) {
		t.Fatalf("Commit() error = %v, want store error", err)
	}
	if store.saves() != 1 {
		t.Fatalf("Save() count = %d, want 1", store.saves())
	}
	if checkpoint := store.checkpoint("orders-eu"); checkpoint.JournalSequence != 0 || checkpoint.Offset != nil {
		t.Fatalf("checkpoint after failed commit = %#v, want empty", checkpoint)
	}
}

func TestCommandJournalSourceCheckpointValidatesInputsAndContext(t *testing.T) {
	if _, err := NewCommandJournalSourceCheckpointCoordinator(nil, &mz013SourceCheckpointStore{}); !errors.Is(err, ErrNilCommandJournal) {
		t.Fatalf("nil journal error = %v, want ErrNilCommandJournal", err)
	}
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	if _, err := NewCommandJournalSourceCheckpointCoordinator(journal, nil); !errors.Is(err, ErrNilCommandJournalSourceCheckpointStore) {
		t.Fatalf("nil store error = %v, want ErrNilCommandJournalSourceCheckpointStore", err)
	}
	store := &mz013SourceCheckpointStore{}
	coordinator, err := NewCommandJournalSourceCheckpointCoordinator(journal, store)
	if err != nil {
		t.Fatalf("NewCommandJournalSourceCheckpointCoordinator() error = %v", err)
	}
	if _, err := coordinator.Load(context.Background(), " "); !errors.Is(err, ErrInvalidCommandJournalSourceID) {
		t.Fatalf("empty source ID error = %v, want ErrInvalidCommandJournalSourceID", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := coordinator.Commit(canceled, "orders-eu", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Commit() error = %v, want context.Canceled", err)
	}
	if store.saves() != 0 {
		t.Fatalf("Save() count after canceled commit = %d, want 0", store.saves())
	}
	tooLargeOffset := make([]byte, MaxCommandJournalSourceCheckpointOffsetBytes+1)
	if _, err := coordinator.Commit(context.Background(), "orders-eu", tooLargeOffset); !errors.Is(err, ErrCommandJournalSourceCheckpointOffsetTooLarge) {
		t.Fatalf("oversized offset error = %v, want ErrCommandJournalSourceCheckpointOffsetTooLarge", err)
	}
	if store.saves() != 0 {
		t.Fatalf("Save() count after oversized offset = %d, want 0", store.saves())
	}
}
