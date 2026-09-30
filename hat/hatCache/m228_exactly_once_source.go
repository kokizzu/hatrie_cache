package hatCache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	ErrCommandJournalExactlyOnceSourceNil                 = errors.New("hatriecache: exactly-once source coordinator is nil")
	ErrCommandJournalExactlyOnceSourceIdempotencyDisabled = errors.New("hatriecache: exactly-once source requires journal idempotency")
	ErrCommandJournalExactlyOnceSourceBatchInvalid        = errors.New("hatriecache: exactly-once source batch is invalid")
	ErrCommandJournalExactlyOnceSourceTransactionConflict = errors.New("hatriecache: exactly-once source transaction conflicts with its checkpoint")
	ErrCommandJournalExactlyOnceSourceCommand             = errors.New("hatriecache: exactly-once source command failed")
)

// CommandJournalExactlyOnceSourceBatch is one source transaction. Commands
// are applied as one atomic BATCH journal entry, then the source offset is
// committed under the journal persistence barrier.
type CommandJournalExactlyOnceSourceBatch struct {
	SourceID      string
	TransactionID string
	Offset        []byte
	Commands      []CacheCommandRequest
}

// CommandJournalExactlyOnceSourceResult reports a source batch checkpoint.
// AlreadyCommitted is true when the same transaction and fingerprint were
// already durably checkpointed, so no command was executed again.
type CommandJournalExactlyOnceSourceResult struct {
	Checkpoint       CommandJournalSourceCheckpoint
	AlreadyCommitted bool
}

// CommandJournalExactlyOnceSourceCoordinator atomically applies one source
// transaction and checkpoints its offset. It is opt-in and requires a journal
// opened with IdempotencyCapacity > 0. The journal's durable idempotency
// record closes the crash window between the atomic batch and checkpoint save.
type CommandJournalExactlyOnceSourceCoordinator struct {
	mu         sync.Mutex
	journal    *CommandJournal
	checkpoint *CommandJournalSourceCheckpointCoordinator
}

// NewCommandJournalExactlyOnceSourceCoordinator creates an opt-in exactly-once
// source coordinator. A journal without idempotency is rejected because a
// source retry after a process restart could otherwise apply its batch twice.
func NewCommandJournalExactlyOnceSourceCoordinator(journal *CommandJournal, store CommandJournalSourceCheckpointStore) (*CommandJournalExactlyOnceSourceCoordinator, error) {
	if journal == nil {
		return nil, ErrNilCommandJournal
	}
	if !journal.idempotency.enabled() {
		return nil, ErrCommandJournalExactlyOnceSourceIdempotencyDisabled
	}
	checkpoint, err := NewCommandJournalSourceCheckpointCoordinator(journal, store)
	if err != nil {
		return nil, err
	}
	return &CommandJournalExactlyOnceSourceCoordinator{
		journal:    journal,
		checkpoint: checkpoint,
	}, nil
}

// ApplyBatch applies and durably checkpoints one source transaction. A failed
// checkpoint leaves the journaled batch durable but leaves the source
// checkpoint unchanged; retrying the same batch reuses its durable
// idempotency fingerprint and does not apply the commands a second time.
func (coordinator *CommandJournalExactlyOnceSourceCoordinator) ApplyBatch(ctx context.Context, trie *HatTrie, batch CommandJournalExactlyOnceSourceBatch) (CommandJournalExactlyOnceSourceResult, error) {
	if coordinator == nil || coordinator.journal == nil || coordinator.checkpoint == nil {
		return CommandJournalExactlyOnceSourceResult{}, ErrCommandJournalExactlyOnceSourceNil
	}
	if trie == nil {
		return CommandJournalExactlyOnceSourceResult{}, ErrNilHatTrie
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return CommandJournalExactlyOnceSourceResult{}, err
	}
	normalized, request, fingerprint, err := normalizeExactlyOnceSourceBatch(batch)
	if err != nil {
		return CommandJournalExactlyOnceSourceResult{}, err
	}

	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	checkpoint, err := coordinator.checkpoint.Load(ctx, normalized.SourceID)
	if err != nil {
		return CommandJournalExactlyOnceSourceResult{}, err
	}
	if checkpoint.TransactionID == normalized.TransactionID && checkpoint.TransactionID != "" {
		if !bytesEqual(checkpoint.BatchFingerprint, fingerprint) || !bytesEqual(checkpoint.Offset, normalized.Offset) {
			return CommandJournalExactlyOnceSourceResult{}, ErrCommandJournalExactlyOnceSourceTransactionConflict
		}
		return CommandJournalExactlyOnceSourceResult{
			Checkpoint:       checkpoint,
			AlreadyCommitted: true,
		}, nil
	}
	if err := ctx.Err(); err != nil {
		return CommandJournalExactlyOnceSourceResult{}, err
	}
	response := coordinator.journal.ExecuteCommand(trie, request)
	if !response.OK {
		return CommandJournalExactlyOnceSourceResult{}, fmt.Errorf("%w: %s", ErrCommandJournalExactlyOnceSourceCommand, response.Message)
	}
	committed, err := coordinator.checkpoint.CommitTransaction(ctx, normalized.SourceID, normalized.TransactionID, normalized.Offset, fingerprint)
	if err != nil {
		return CommandJournalExactlyOnceSourceResult{}, err
	}
	return CommandJournalExactlyOnceSourceResult{Checkpoint: committed}, nil
}

func normalizeExactlyOnceSourceBatch(batch CommandJournalExactlyOnceSourceBatch) (CommandJournalExactlyOnceSourceBatch, CacheCommandRequest, []byte, error) {
	batch.SourceID = strings.TrimSpace(batch.SourceID)
	batch.TransactionID = strings.TrimSpace(batch.TransactionID)
	if err := validateCommandJournalSourceID(batch.SourceID); err != nil {
		return CommandJournalExactlyOnceSourceBatch{}, CacheCommandRequest{}, nil, err
	}
	if batch.TransactionID == "" {
		return CommandJournalExactlyOnceSourceBatch{}, CacheCommandRequest{}, nil, ErrInvalidCommandJournalSourceTransactionID
	}
	if err := validateCommandJournalSourceCheckpointMetadata(batch.TransactionID, make([]byte, CommandJournalSourceCheckpointFingerprintBytes)); err != nil {
		return CommandJournalExactlyOnceSourceBatch{}, CacheCommandRequest{}, nil, err
	}
	if len(batch.Offset) > MaxCommandJournalSourceCheckpointOffsetBytes {
		return CommandJournalExactlyOnceSourceBatch{}, CacheCommandRequest{}, nil, ErrCommandJournalSourceCheckpointOffsetTooLarge
	}
	if len(batch.Commands) == 0 || len(batch.Commands) > maxPublicCommandBatchSize {
		return CommandJournalExactlyOnceSourceBatch{}, CacheCommandRequest{}, nil, ErrCommandJournalExactlyOnceSourceBatchInvalid
	}
	commands := make([]CacheCommandRequest, len(batch.Commands))
	for index, command := range batch.Commands {
		if validatePublicCommandBatchPayload(command, index) != nil {
			return CommandJournalExactlyOnceSourceBatch{}, CacheCommandRequest{}, nil, ErrCommandJournalExactlyOnceSourceBatchInvalid
		}
		if !commandShouldJournal(command) {
			return CommandJournalExactlyOnceSourceBatch{}, CacheCommandRequest{}, nil, ErrCommandJournalExactlyOnceSourceBatchInvalid
		}
		commands[index] = cloneAsyncCommandRequest(command)
	}
	keyDigest := sha256.Sum256([]byte("hatrie:m228:" + batch.SourceID + "\x00" + batch.TransactionID))
	request := CacheCommandRequest{
		Command:        "BATCH",
		Atomic:         true,
		Batch:          commands,
		IdempotencyKey: "m228:" + hex.EncodeToString(keyDigest[:]),
	}
	check, err := newCommandIdempotencyCheck(request)
	if err != nil {
		return CommandJournalExactlyOnceSourceBatch{}, CacheCommandRequest{}, nil, err
	}
	fingerprint := append([]byte(nil), check.fingerprint[:]...)
	batch.Commands = commands
	batch.Offset = cloneCommandJournalSourceCheckpointOffset(batch.Offset)
	return batch, request, fingerprint, nil
}

func bytesEqual(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
