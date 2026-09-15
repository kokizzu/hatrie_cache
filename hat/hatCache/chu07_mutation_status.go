package hatCache

import (
	"errors"
	"fmt"
)

var ErrCommandJournalMutationNotFound = errors.New("hatriecache: command journal mutation was not found")

// CommandJournalMutationState describes the durable outcome of one journal
// mutation. Pending, rejected, and failed are also used by live asynchronous
// submissions; only committed mutations are recoverable from the journal.
type CommandJournalMutationState string

const (
	CommandJournalMutationUnknown   CommandJournalMutationState = "unknown"
	CommandJournalMutationPending   CommandJournalMutationState = "pending"
	CommandJournalMutationCommitted CommandJournalMutationState = "committed"
	CommandJournalMutationRejected  CommandJournalMutationState = "rejected"
	CommandJournalMutationFailed    CommandJournalMutationState = "failed"
)

// CommandJournalMutationStatus is a privacy-safe lifecycle snapshot. Progress
// is one for a committed journal record and zero for an incomplete operation.
type CommandJournalMutationStatus struct {
	Sequence uint64                      `json:"sequence"`
	Command  string                      `json:"command,omitempty"`
	Key      string                      `json:"key,omitempty"`
	State    CommandJournalMutationState `json:"state"`
	Progress uint64                      `json:"progress"`
	Message  string                      `json:"message,omitempty"`
}

// MutationStatus looks up one durable journal mutation by sequence. The
// lookup rereads the framed journal, so it continues to work after a process
// restart without retaining an in-memory map of every historical mutation.
func (journal *CommandJournal) MutationStatus(sequence uint64) (CommandJournalMutationStatus, error) {
	if journal == nil {
		return CommandJournalMutationStatus{}, ErrNilCommandJournal
	}
	if sequence == 0 {
		return CommandJournalMutationStatus{}, fmt.Errorf("%w: sequence must be positive", ErrCommandJournalMutationNotFound)
	}
	tail, err := journal.Tail(sequence-1, 1)
	if err != nil {
		return CommandJournalMutationStatus{}, err
	}
	if len(tail.Entries) != 1 || tail.Entries[0].Sequence != sequence {
		return CommandJournalMutationStatus{}, fmt.Errorf("%w: sequence %d", ErrCommandJournalMutationNotFound, sequence)
	}
	return commandJournalMutationStatusFromRecord(tail.Entries[0]), nil
}

func commandJournalMutationStatusFromRecord(record CommandJournalRecord) CommandJournalMutationStatus {
	return CommandJournalMutationStatus{
		Sequence: record.Sequence,
		Command:  record.Request.Command,
		Key:      record.Request.Key,
		State:    CommandJournalMutationCommitted,
		Progress: 1,
	}
}
