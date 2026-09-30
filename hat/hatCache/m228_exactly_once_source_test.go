package hatCache

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

var errM228CheckpointStore = errors.New("m228 checkpoint store failure")

type m228SourceCheckpointStore struct {
	mu         sync.Mutex
	checkpoint map[string]CommandJournalSourceCheckpoint
	saveErr    error
	saveCount  int
}

func (store *m228SourceCheckpointStore) Load(_ context.Context, sourceID string) (CommandJournalSourceCheckpoint, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	checkpoint := store.checkpoint[sourceID]
	checkpoint.Offset = append([]byte(nil), checkpoint.Offset...)
	checkpoint.BatchFingerprint = append([]byte(nil), checkpoint.BatchFingerprint...)
	return checkpoint, nil
}

func (store *m228SourceCheckpointStore) Save(_ context.Context, sourceID string, checkpoint CommandJournalSourceCheckpoint) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.saveCount++
	if store.saveErr != nil {
		return store.saveErr
	}
	if store.checkpoint == nil {
		store.checkpoint = make(map[string]CommandJournalSourceCheckpoint)
	}
	checkpoint.Offset = append([]byte(nil), checkpoint.Offset...)
	checkpoint.BatchFingerprint = append([]byte(nil), checkpoint.BatchFingerprint...)
	store.checkpoint[sourceID] = checkpoint
	return nil
}

func (store *m228SourceCheckpointStore) current(sourceID string) CommandJournalSourceCheckpoint {
	store.mu.Lock()
	defer store.mu.Unlock()
	checkpoint := store.checkpoint[sourceID]
	checkpoint.Offset = append([]byte(nil), checkpoint.Offset...)
	checkpoint.BatchFingerprint = append([]byte(nil), checkpoint.BatchFingerprint...)
	return checkpoint
}

func TestM228ExactlyOnceSourceRestartAfterCheckpointFailure(t *testing.T) {
	journalPath := filepath.Join(t.TempDir(), "source.journal")
	options := CommandJournalOptions{GroupCommitMaxBatch: 1, IdempotencyCapacity: 8}
	journal, err := OpenCommandJournalWithOptions(journalPath, options)
	if err != nil {
		t.Fatal(err)
	}
	trie := newTestTrie(t)
	store := &m228SourceCheckpointStore{}
	coordinator, err := NewCommandJournalExactlyOnceSourceCoordinator(journal, store)
	if err != nil {
		t.Fatal(err)
	}
	batch := CommandJournalExactlyOnceSourceBatch{
		SourceID:      "orders-eu",
		TransactionID: "tx-1",
		Offset:        []byte{0, 1, 255},
		Commands:      []CacheCommandRequest{{Command: "SETINT", Key: "orders", Value: "1"}},
	}
	store.saveErr = errM228CheckpointStore
	if _, err := coordinator.ApplyBatch(context.Background(), trie, batch); !errors.Is(err, errM228CheckpointStore) {
		t.Fatalf("ApplyBatch() error = %v, want checkpoint failure", err)
	}
	if got := trie.GetCounter("orders"); got != 1 {
		t.Fatalf("first application count = %d, want 1", got)
	}
	if checkpoint := store.current(batch.SourceID); checkpoint.TransactionID != "" {
		t.Fatalf("failed commit published checkpoint = %#v", checkpoint)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenCommandJournalWithOptions(journalPath, options)
	if err != nil {
		t.Fatal(err)
	}
	recoveredTrie := newTestTrie(t)
	if _, err := reopened.Replay(recoveredTrie, 0); err != nil {
		t.Fatal(err)
	}
	store.saveErr = nil
	restarted, err := NewCommandJournalExactlyOnceSourceCoordinator(reopened, store)
	if err != nil {
		t.Fatal(err)
	}
	result, err := restarted.ApplyBatch(context.Background(), recoveredTrie, batch)
	if err != nil {
		t.Fatal(err)
	}
	if result.AlreadyCommitted {
		t.Fatal("retry was marked already committed before the checkpoint was saved")
	}
	if result.Checkpoint.TransactionID != batch.TransactionID || !bytes.Equal(result.Checkpoint.Offset, batch.Offset) {
		t.Fatalf("checkpoint = %#v, want transaction and offset", result.Checkpoint)
	}
	if got := recoveredTrie.GetCounter("orders"); got != 1 {
		t.Fatalf("restarted application count = %d, want exactly once", got)
	}

	duplicate, err := restarted.ApplyBatch(context.Background(), recoveredTrie, batch)
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate.AlreadyCommitted {
		t.Fatal("duplicate committed source batch was not recognized")
	}
	if got := recoveredTrie.GetCounter("orders"); got != 1 {
		t.Fatalf("duplicate committed application count = %d, want exactly once", got)
	}
	conflictingOffset := batch
	conflictingOffset.Offset = []byte{0, 1, 254}
	if _, err := restarted.ApplyBatch(context.Background(), recoveredTrie, conflictingOffset); !errors.Is(err, ErrCommandJournalExactlyOnceSourceTransactionConflict) {
		t.Fatalf("conflicting offset error = %v, want transaction conflict", err)
	}
	conflictingCommand := batch
	conflictingCommand.Commands = []CacheCommandRequest{{Command: "SETINT", Key: "orders", Value: "2"}}
	if _, err := restarted.ApplyBatch(context.Background(), recoveredTrie, conflictingCommand); !errors.Is(err, ErrCommandJournalExactlyOnceSourceTransactionConflict) {
		t.Fatalf("conflicting command error = %v, want transaction conflict", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestM228ExactlyOnceSourceRequiresJournalIdempotency(t *testing.T) {
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "source.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	if _, err := NewCommandJournalExactlyOnceSourceCoordinator(journal, &m228SourceCheckpointStore{}); !errors.Is(err, ErrCommandJournalExactlyOnceSourceIdempotencyDisabled) {
		t.Fatalf("constructor error = %v, want idempotency-disabled error", err)
	}
}

func TestM228ExactlyOnceSourceAtomicRollback(t *testing.T) {
	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "source.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 1,
		IdempotencyCapacity: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	trie := newTestTrie(t)
	store := &m228SourceCheckpointStore{}
	coordinator, err := NewCommandJournalExactlyOnceSourceCoordinator(journal, store)
	if err != nil {
		t.Fatal(err)
	}
	_, err = coordinator.ApplyBatch(context.Background(), trie, CommandJournalExactlyOnceSourceBatch{
		SourceID:      "orders-eu",
		TransactionID: "tx-rollback",
		Offset:        []byte{2},
		Commands: []CacheCommandRequest{
			{Command: "CREATEFW", Key: "rollback", Value: "1"},
			{Command: "ADDFW", Key: "rollback", Value: "2", Subkey: "1"},
		},
	})
	if !errors.Is(err, ErrCommandJournalExactlyOnceSourceCommand) {
		t.Fatalf("ApplyBatch() error = %v, want command error", err)
	}
	if got := trie.GetString("rollback"); got != "" {
		t.Fatalf("rolled-back string = %q, want empty", got)
	}
	if checkpoint := store.current("orders-eu"); checkpoint.TransactionID != "" {
		t.Fatalf("failed batch published checkpoint = %#v", checkpoint)
	}
}

func TestM228ExactlyOnceSourceRejectsUnsafeBatchPayloads(t *testing.T) {
	tooLarge := CommandJournalExactlyOnceSourceBatch{
		SourceID:      "orders-eu",
		TransactionID: "tx-large",
		Commands:      make([]CacheCommandRequest, maxPublicCommandBatchSize+1),
	}
	if _, _, _, err := normalizeExactlyOnceSourceBatch(tooLarge); !errors.Is(err, ErrCommandJournalExactlyOnceSourceBatchInvalid) {
		t.Fatalf("oversized batch error = %v, want invalid batch", err)
	}
	nested := CommandJournalExactlyOnceSourceBatch{
		SourceID:      "orders-eu",
		TransactionID: "tx-nested",
		Commands: []CacheCommandRequest{{
			Command: "BATCH",
			Batch:   []CacheCommandRequest{{Command: "SETINT", Key: "orders", Value: "1"}},
		}},
	}
	if _, _, _, err := normalizeExactlyOnceSourceBatch(nested); !errors.Is(err, ErrCommandJournalExactlyOnceSourceBatchInvalid) {
		t.Fatalf("nested batch error = %v, want invalid batch", err)
	}
}

func TestM228BinaryJournalTailPreservesAtomicBatch(t *testing.T) {
	tail := CommandJournalTail{
		LastSequence: 1,
		Limit:        1,
		Entries: []CommandJournalRecord{{
			Sequence: 1,
			Request: CacheCommandRequest{
				Command:        "BATCH",
				Atomic:         true,
				IdempotencyKey: "tail-key",
				Batch: []CacheCommandRequest{{
					Command: "SETINT",
					Key:     "orders",
					Value:   "1",
				}},
			},
		}},
	}
	data, err := marshalCommandJournalTailBinary(tail)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeCommandJournalTailBinaryData(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Entries) != 1 || !decoded.Entries[0].Request.Atomic || len(decoded.Entries[0].Request.Batch) != 1 {
		t.Fatalf("decoded tail = %#v, want atomic batch", decoded)
	}
	if decoded.Entries[0].Request.Batch[0].Command != "SETINT" || decoded.Entries[0].Request.Batch[0].Value != "1" {
		t.Fatalf("decoded batch request = %#v", decoded.Entries[0].Request.Batch[0])
	}
}
