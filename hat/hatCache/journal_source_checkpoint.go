package hatCache

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

const MaxCommandJournalSourceCheckpointOffsetBytes = 1 << 20

var (
	ErrNilCommandJournalSourceCheckpointStore       = errors.New("hatriecache: source checkpoint store is nil")
	ErrInvalidCommandJournalSourceID                = errors.New("hatriecache: source checkpoint source ID is invalid")
	ErrCommandJournalSourceCheckpointOffsetTooLarge = errors.New("hatriecache: source checkpoint offset is too large")
	ErrCommandJournalSourceCheckpointAhead          = errors.New("hatriecache: source checkpoint is ahead of the journal")
)

// CommandJournalSourceCheckpoint is the durable source position associated
// with a fully applied journal sequence. Offset is binary-safe and is copied
// by the coordinator before it is handed to a store.
type CommandJournalSourceCheckpoint struct {
	Offset          []byte `json:"offset,omitempty"`
	JournalSequence uint64 `json:"journal_sequence"`
}

// CommandJournalSourceCheckpointStore loads and durably saves a position for
// one source. Save must finish persistence before returning and must not call
// back into the journal because it runs inside WithPersistenceBarrier.
type CommandJournalSourceCheckpointStore interface {
	Load(context.Context, string) (CommandJournalSourceCheckpoint, error)
	Save(context.Context, string, CommandJournalSourceCheckpoint) error
}

// CommandJournalSourceCheckpointCoordinator binds source offsets to the
// journal's latest fully applied sequence. It is opt-in and adds no cost to
// ordinary journal writes.
type CommandJournalSourceCheckpointCoordinator struct {
	journal *CommandJournal
	store   CommandJournalSourceCheckpointStore
}

// NewCommandJournalSourceCheckpointCoordinator creates an opt-in source
// checkpoint coordinator.
func NewCommandJournalSourceCheckpointCoordinator(journal *CommandJournal, store CommandJournalSourceCheckpointStore) (*CommandJournalSourceCheckpointCoordinator, error) {
	if journal == nil {
		return nil, ErrNilCommandJournal
	}
	if store == nil {
		return nil, ErrNilCommandJournalSourceCheckpointStore
	}
	return &CommandJournalSourceCheckpointCoordinator{journal: journal, store: store}, nil
}

// Load reads a source checkpoint and verifies that its journal sequence is
// available locally. A zero checkpoint means that the source has no saved
// position yet.
func (coordinator *CommandJournalSourceCheckpointCoordinator) Load(ctx context.Context, sourceID string) (CommandJournalSourceCheckpoint, error) {
	if coordinator == nil || coordinator.journal == nil {
		return CommandJournalSourceCheckpoint{}, ErrNilCommandJournal
	}
	if coordinator.store == nil {
		return CommandJournalSourceCheckpoint{}, ErrNilCommandJournalSourceCheckpointStore
	}
	if err := validateCommandJournalSourceID(sourceID); err != nil {
		return CommandJournalSourceCheckpoint{}, err
	}
	ctx = normalizeCommandJournalSourceCheckpointContext(ctx)
	if err := ctx.Err(); err != nil {
		return CommandJournalSourceCheckpoint{}, err
	}
	checkpoint, err := coordinator.store.Load(ctx, sourceID)
	if err != nil {
		return CommandJournalSourceCheckpoint{}, err
	}
	if len(checkpoint.Offset) > MaxCommandJournalSourceCheckpointOffsetBytes {
		return CommandJournalSourceCheckpoint{}, ErrCommandJournalSourceCheckpointOffsetTooLarge
	}
	checkpoint.Offset = cloneCommandJournalSourceCheckpointOffset(checkpoint.Offset)
	journalSequence := coordinator.journal.Sequence()
	if checkpoint.JournalSequence > journalSequence {
		return CommandJournalSourceCheckpoint{}, fmt.Errorf("%w: checkpoint sequence %d, journal sequence %d", ErrCommandJournalSourceCheckpointAhead, checkpoint.JournalSequence, journalSequence)
	}
	return checkpoint, nil
}

// Commit durably saves offset at the latest fully applied journal sequence.
// Callers should apply a source batch through the journal before calling
// Commit. If persistence fails, the offset is not advanced and replaying the
// source batch is the safe recovery behavior.
func (coordinator *CommandJournalSourceCheckpointCoordinator) Commit(ctx context.Context, sourceID string, offset []byte) (CommandJournalSourceCheckpoint, error) {
	if coordinator == nil || coordinator.journal == nil {
		return CommandJournalSourceCheckpoint{}, ErrNilCommandJournal
	}
	if coordinator.store == nil {
		return CommandJournalSourceCheckpoint{}, ErrNilCommandJournalSourceCheckpointStore
	}
	if err := validateCommandJournalSourceID(sourceID); err != nil {
		return CommandJournalSourceCheckpoint{}, err
	}
	if len(offset) > MaxCommandJournalSourceCheckpointOffsetBytes {
		return CommandJournalSourceCheckpoint{}, ErrCommandJournalSourceCheckpointOffsetTooLarge
	}
	ctx = normalizeCommandJournalSourceCheckpointContext(ctx)
	if err := ctx.Err(); err != nil {
		return CommandJournalSourceCheckpoint{}, err
	}
	checkpoint := CommandJournalSourceCheckpoint{Offset: cloneCommandJournalSourceCheckpointOffset(offset)}
	err := coordinator.journal.WithPersistenceBarrier(func(sequence uint64) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		checkpoint.JournalSequence = sequence
		return coordinator.store.Save(ctx, sourceID, checkpoint)
	})
	if err != nil {
		return CommandJournalSourceCheckpoint{}, err
	}
	return checkpoint, nil
}

func validateCommandJournalSourceID(sourceID string) error {
	if strings.TrimSpace(sourceID) == "" {
		return ErrInvalidCommandJournalSourceID
	}
	return nil
}

func normalizeCommandJournalSourceCheckpointContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func cloneCommandJournalSourceCheckpointOffset(offset []byte) []byte {
	if len(offset) == 0 {
		return nil
	}
	return append([]byte(nil), offset...)
}
