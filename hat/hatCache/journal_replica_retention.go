package hatCache

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// MaxCommandJournalReplicaWatermarkNameBytes bounds the caller-controlled
// replica identifier retained in the in-memory acknowledgment map.
const MaxCommandJournalReplicaWatermarkNameBytes = 256

// CommandJournalReplicaWatermark reports one replica's durable journal
// progress. Records at or before Sequence may be compacted; later records
// remain retained until every registered replica advances.
type CommandJournalReplicaWatermark struct {
	Replica  string `json:"replica"`
	Sequence uint64 `json:"sequence"`
	Lag      uint64 `json:"lag"`
}

// SetReplicaWatermark records a replica's durable applied sequence. The
// sequence can only advance and cannot exceed the current journal tail. The
// minimum sequence across registered replicas protects the journal from
// deleting history that a lagging replica still needs.
//
// Watermarks are process-local. Replication code that persists its own
// acknowledgment checkpoint should restore it after opening the journal and
// before relying on a narrow retention budget.
func (journal *CommandJournal) SetReplicaWatermark(replica string, sequence uint64) error {
	if journal == nil {
		return ErrNilCommandJournal
	}
	replica = strings.TrimSpace(replica)
	if replica == "" {
		return errors.New("hatriecache: replica watermark name is required")
	}
	if len(replica) > MaxCommandJournalReplicaWatermarkNameBytes {
		return fmt.Errorf("hatriecache: replica watermark name must be <= %d bytes", MaxCommandJournalReplicaWatermarkNameBytes)
	}

	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed {
		return ErrCommandJournalClosed
	}
	lastSequence := journal.lastSequenceLocked()
	if sequence > lastSequence {
		return fmt.Errorf("hatriecache: replica watermark %q sequence %d exceeds journal tail %d", replica, sequence, lastSequence)
	}
	if prior, exists := journal.replicaWatermarks[replica]; exists && sequence < prior {
		return fmt.Errorf("hatriecache: replica watermark %q cannot move backward from %d to %d", replica, prior, sequence)
	}
	if journal.replicaWatermarks == nil {
		journal.replicaWatermarks = make(map[string]uint64)
	}
	journal.replicaWatermarks[replica] = sequence
	return nil
}

// RemoveReplicaWatermark stops protecting journal history for replica. It is
// intentionally explicit because the replica may require a fresh snapshot.
func (journal *CommandJournal) RemoveReplicaWatermark(replica string) bool {
	if journal == nil {
		return false
	}
	replica = strings.TrimSpace(replica)
	if replica == "" {
		return false
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed || journal.replicaWatermarks == nil {
		return false
	}
	if _, exists := journal.replicaWatermarks[replica]; !exists {
		return false
	}
	delete(journal.replicaWatermarks, replica)
	return true
}

// ReplicaWatermarks returns a sorted immutable status snapshot. Lag is the
// number of journal sequences after the replica's durable checkpoint.
func (journal *CommandJournal) ReplicaWatermarks() []CommandJournalReplicaWatermark {
	if journal == nil {
		return nil
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if len(journal.replicaWatermarks) == 0 {
		return nil
	}
	watermarks := make([]CommandJournalReplicaWatermark, 0, len(journal.replicaWatermarks))
	lastSequence := journal.lastSequenceLocked()
	for replica, sequence := range journal.replicaWatermarks {
		lag := uint64(0)
		if lastSequence > sequence {
			lag = lastSequence - sequence
		}
		watermarks = append(watermarks, CommandJournalReplicaWatermark{Replica: replica, Sequence: sequence, Lag: lag})
	}
	sort.Slice(watermarks, func(left, right int) bool { return watermarks[left].Replica < watermarks[right].Replica })
	return watermarks
}

func (journal *CommandJournal) replicaRetentionThroughLocked() (uint64, bool) {
	var through uint64
	found := false
	for _, sequence := range journal.replicaWatermarks {
		if !found || sequence < through {
			through = sequence
			found = true
		}
	}
	return through, found
}
