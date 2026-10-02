package hatCache

import (
	"time"
)

// CommandJournalDurability reports the applied and most recently synced
// journal sequences. A non-zero pending sequence is expected for periodic or
// disabled spaces until Sync is called or the periodic write boundary runs.
type CommandJournalDurability struct {
	AppliedSequence uint64    `json:"applied_sequence"`
	DurableSequence uint64    `json:"durable_sequence"`
	Pending         bool      `json:"pending"`
	LastSyncAt      time.Time `json:"last_sync_at,omitempty"`
	LastSyncError   string    `json:"last_sync_error,omitempty"`
}

// Durability returns a lock-consistent durability snapshot without changing
// the journal or forcing a filesystem sync.
func (journal *CommandJournal) Durability() CommandJournalDurability {
	if journal == nil {
		return CommandJournalDurability{}
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	return CommandJournalDurability{
		AppliedSequence: journal.lastSequenceLocked(),
		DurableSequence: journal.lastSyncedSequence,
		Pending:         journal.lastSyncedSequence < journal.lastSequenceLocked(),
		LastSyncAt:      journal.lastSyncAt,
		LastSyncError:   journal.lastSyncError,
	}
}

// Sync forces the current journal tail to the filesystem and returns the
// resulting durable sequence. It is the explicit boundary for periodic and
// disabled spaces when the caller needs a durability guarantee.
func (journal *CommandJournal) Sync() (uint64, error) {
	if journal == nil {
		return 0, ErrNilCommandJournal
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed {
		return journal.lastSyncedSequence, ErrCommandJournalClosed
	}
	if err := journal.syncLocked(); err != nil {
		return journal.lastSyncedSequence, err
	}
	return journal.lastSyncedSequence, nil
}
