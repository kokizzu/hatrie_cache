package hatCache

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type mu34CheckpointStore struct {
	mu          sync.Mutex
	checkpoints map[string]CommandJournalSourceCheckpoint
	saveErr     error
}

func (store *mu34CheckpointStore) Load(_ context.Context, sourceID string) (CommandJournalSourceCheckpoint, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	checkpoint := store.checkpoints[sourceID]
	checkpoint.Offset = append([]byte(nil), checkpoint.Offset...)
	return checkpoint, nil
}

func (store *mu34CheckpointStore) Save(_ context.Context, sourceID string, checkpoint CommandJournalSourceCheckpoint) error {
	store.mu.Lock()
	defer store.mu.Unlock()
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

func (store *mu34CheckpointStore) checkpoint(sourceID string) CommandJournalSourceCheckpoint {
	store.mu.Lock()
	defer store.mu.Unlock()
	checkpoint := store.checkpoints[sourceID]
	checkpoint.Offset = append([]byte(nil), checkpoint.Offset...)
	return checkpoint
}

func TestMU34HistoricalSubscriptionCancellationResumesAfterCommittedSequence(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	store := &mu34CheckpointStore{}
	for index := 1; index <= 3; index++ {
		response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     "orders/" + string(rune('0'+index)),
			Value:   "value",
		})
		if !response.OK {
			t.Fatalf("SETSTR %d failed: %#v", index, response)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	subscription, err := journal.SubscribeHistoricalCommandJournal(ctx, CommandJournalHistoricalSubscriptionOptions{
		SourceID:     "orders-consumer",
		Store:        store,
		ReplayLimit:  8,
		Buffer:       1,
		PollInterval: time.Millisecond,
		KeyPrefix:    "orders/",
	})
	if err != nil {
		cancel()
		t.Fatalf("SubscribeHistoricalCommandJournal() error = %v", err)
	}

	first, ok, err := subscription.Next(context.Background())
	if err != nil || !ok || first.Sequence != 1 {
		t.Fatalf("first Next() = %#v, %v, %v; want sequence 1", first, ok, err)
	}
	checkpoint, err := subscription.Commit(context.Background())
	if err != nil || checkpoint.JournalSequence != 1 {
		t.Fatalf("first Commit() = %#v, %v; want durable sequence 1", checkpoint, err)
	}

	second, ok, err := subscription.Next(context.Background())
	if err != nil || !ok || second.Sequence != 2 {
		t.Fatalf("second Next() = %#v, %v, %v; want sequence 2", second, ok, err)
	}
	cancel()
	subscription.Close()

	resumed, err := journal.SubscribeHistoricalCommandJournal(context.Background(), CommandJournalHistoricalSubscriptionOptions{
		SourceID:     "orders-consumer",
		Store:        store,
		ReplayLimit:  8,
		Buffer:       1,
		PollInterval: time.Millisecond,
		KeyPrefix:    "orders/",
	})
	if err != nil {
		t.Fatalf("resumed SubscribeHistoricalCommandJournal() error = %v", err)
	}
	defer resumed.Close()

	replayedSecond, ok, err := resumed.Next(context.Background())
	if err != nil || !ok || replayedSecond.Sequence != 2 {
		t.Fatalf("replayed second Next() = %#v, %v, %v; want uncommitted sequence 2", replayedSecond, ok, err)
	}
	if _, err := resumed.Commit(context.Background()); err != nil {
		t.Fatalf("second Commit() error = %v", err)
	}
	replayedThird, ok, err := resumed.Next(context.Background())
	if err != nil || !ok || replayedThird.Sequence != 3 {
		t.Fatalf("replayed third Next() = %#v, %v, %v; want sequence 3", replayedThird, ok, err)
	}
	if _, err := resumed.Commit(context.Background()); err != nil {
		t.Fatalf("third Commit() error = %v", err)
	}
	if got := store.checkpoint("orders-consumer").JournalSequence; got != 3 {
		t.Fatalf("stored checkpoint sequence = %d, want 3", got)
	}
}

func TestMU34HistoricalSubscriptionCommitFailureDoesNotAdvance(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	store := &mu34CheckpointStore{}
	response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "orders/1", Value: "value"})
	if !response.OK {
		t.Fatalf("SETSTR failed: %#v", response)
	}

	subscription, err := journal.SubscribeHistoricalCommandJournal(context.Background(), CommandJournalHistoricalSubscriptionOptions{
		SourceID:    "orders-consumer",
		Store:       store,
		ReplayLimit: 8,
	})
	if err != nil {
		t.Fatalf("SubscribeHistoricalCommandJournal() error = %v", err)
	}
	defer subscription.Close()
	if _, ok, err := subscription.Next(context.Background()); err != nil || !ok {
		t.Fatalf("Next() = %v, %v; want one record", ok, err)
	}

	store.mu.Lock()
	store.saveErr = errors.New("checkpoint unavailable")
	store.mu.Unlock()
	if _, err := subscription.Commit(context.Background()); err == nil {
		t.Fatal("Commit() succeeded while checkpoint store failed")
	}
	if got := subscription.Checkpoint().JournalSequence; got != 0 {
		t.Fatalf("checkpoint after failed commit = %d, want 0", got)
	}
}
