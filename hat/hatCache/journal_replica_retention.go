package hatCache

import (
	"errors"
	"sort"
	"strings"
)

const MaxCommandJournalReplicaRetentionIDBytes = 256

var (
	ErrCommandJournalReplicaRetentionDisabled         = errors.New("hatriecache: replica retention is disabled")
	ErrCommandJournalReplicaRetentionCapacityExceeded = errors.New("hatriecache: replica retention capacity exceeded")
	ErrCommandJournalReplicaRetentionIDInvalid        = errors.New("hatriecache: replica retention id is invalid")
	ErrCommandJournalReplicaRetentionFuture           = errors.New("hatriecache: replica retention sequence is beyond the journal")
	ErrCommandJournalReplicaRetentionRegression       = errors.New("hatriecache: replica retention acknowledgement regressed")
	ErrCommandJournalReplicaRetentionNotRegistered   = errors.New("hatriecache: replica retention id is not registered")
)

// CommandJournalReplicaRetention reports one registered replica's durable
// replay cursor. Entries are process-local configuration and are intentionally
// bounded by CommandJournalOptions.ReplicaRetentionCapacity.
type CommandJournalReplicaRetention struct {
	ReplicaID          string `json:"replica_id"`
	AcknowledgedThrough uint64 `json:"acknowledged_through"`
	LastSequence       uint64 `json:"last_sequence"`
	Lag                uint64 `json:"lag"`
}

func normalizeCommandJournalReplicaRetentionID(replicaID string) (string, error) {
	replicaID = strings.TrimSpace(replicaID)
	if replicaID == "" || len(replicaID) > MaxCommandJournalReplicaRetentionIDBytes {
		return "", ErrCommandJournalReplicaRetentionIDInvalid
	}
	return replicaID, nil
}

// RegisterReplicaRetention registers a replica and its last acknowledged
// journal sequence. A zero sequence deliberately pins all retained segments.
// Callers should register before appending or rotating when the replica needs
// the complete history from that cursor.
func (journal *CommandJournal) RegisterReplicaRetention(replicaID string, acknowledgedThrough uint64) error {
	if journal == nil {
		return ErrNilCommandJournal
	}
	normalized, err := normalizeCommandJournalReplicaRetentionID(replicaID)
	if err != nil {
		return err
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.replicaRetentionCapacity <= 0 {
		return ErrCommandJournalReplicaRetentionDisabled
	}
	if acknowledgedThrough > journal.lastSequenceLocked() {
		return ErrCommandJournalReplicaRetentionFuture
	}
	if current, exists := journal.replicaRetentions[normalized]; exists {
		if acknowledgedThrough < current {
			return ErrCommandJournalReplicaRetentionRegression
		}
		journal.replicaRetentions[normalized] = acknowledgedThrough
		return journal.pruneSegmentsLocked()
	}
	if len(journal.replicaRetentions) >= journal.replicaRetentionCapacity {
		return ErrCommandJournalReplicaRetentionCapacityExceeded
	}
	journal.replicaRetentions[normalized] = acknowledgedThrough
	return journal.pruneSegmentsLocked()
}

// AcknowledgeReplicaThrough advances one registered replica's replay cursor.
// Acknowledgements are monotonic and cannot name a future journal sequence.
func (journal *CommandJournal) AcknowledgeReplicaThrough(replicaID string, sequence uint64) error {
	if journal == nil {
		return ErrNilCommandJournal
	}
	normalized, err := normalizeCommandJournalReplicaRetentionID(replicaID)
	if err != nil {
		return err
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.replicaRetentionCapacity <= 0 {
		return ErrCommandJournalReplicaRetentionDisabled
	}
	current, exists := journal.replicaRetentions[normalized]
	if !exists {
		return ErrCommandJournalReplicaRetentionNotRegistered
	}
	if sequence > journal.lastSequenceLocked() {
		return ErrCommandJournalReplicaRetentionFuture
	}
	if sequence < current {
		return ErrCommandJournalReplicaRetentionRegression
	}
	if sequence == current {
		return nil
	}
	journal.replicaRetentions[normalized] = sequence
	return journal.pruneSegmentsLocked()
}

// UnregisterReplicaRetention removes a replica cursor and immediately retries
// pruning under the remaining retention constraints.
func (journal *CommandJournal) UnregisterReplicaRetention(replicaID string) error {
	if journal == nil {
		return ErrNilCommandJournal
	}
	normalized, err := normalizeCommandJournalReplicaRetentionID(replicaID)
	if err != nil {
		return err
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.replicaRetentionCapacity <= 0 {
		return ErrCommandJournalReplicaRetentionDisabled
	}
	if _, exists := journal.replicaRetentions[normalized]; !exists {
		return ErrCommandJournalReplicaRetentionNotRegistered
	}
	delete(journal.replicaRetentions, normalized)
	return journal.pruneSegmentsLocked()
}

// ReplicaRetentionSnapshot returns deterministic, detached cursor metadata
// for monitoring and recovery tooling.
func (journal *CommandJournal) ReplicaRetentionSnapshot() []CommandJournalReplicaRetention {
	if journal == nil {
		return nil
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if len(journal.replicaRetentions) == 0 {
		return nil
	}
	lastSequence := journal.lastSequenceLocked()
	entries := make([]CommandJournalReplicaRetention, 0, len(journal.replicaRetentions))
	for replicaID, acknowledgedThrough := range journal.replicaRetentions {
		lag := uint64(0)
		if lastSequence > acknowledgedThrough {
			lag = lastSequence - acknowledgedThrough
		}
		entries = append(entries, CommandJournalReplicaRetention{
			ReplicaID:           replicaID,
			AcknowledgedThrough: acknowledgedThrough,
			LastSequence:        lastSequence,
			Lag:                 lag,
		})
	}
	sort.Slice(entries, func(left, right int) bool {
		return entries[left].ReplicaID < entries[right].ReplicaID
	})
	return entries
}

func (journal *CommandJournal) replicaRetentionThroughLocked() (uint64, bool) {
	if journal == nil || len(journal.replicaRetentions) == 0 {
		return 0, false
	}
	through := ^uint64(0)
	for _, acknowledgedThrough := range journal.replicaRetentions {
		if acknowledgedThrough < through {
			through = acknowledgedThrough
		}
	}
	return through, true
}
