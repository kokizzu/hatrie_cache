package hatCache

import (
	"errors"
	"sort"
	"strings"
)

var (
	ErrReplicaRetentionDisabled         = errors.New("hatriecache: replica retention is disabled")
	ErrReplicaRetentionReplicaRequired  = errors.New("hatriecache: replica retention replica is required")
	ErrReplicaRetentionCapacityExceeded = errors.New("hatriecache: replica retention capacity exceeded")
	ErrReplicaRetentionAckRegressed     = errors.New("hatriecache: replica retention acknowledgement regressed")
	ErrReplicaRetentionSequenceInvalid  = errors.New("hatriecache: replica retention sequence is invalid")
)

const maxReplicaRetentionReplicaBytes = 256

// ReplicaRetentionAcknowledgement is a detached acknowledgement snapshot for
// one replica. Acknowledgements are runtime state; callers should re-ack after
// reopening a journal before expecting retention to advance.
type ReplicaRetentionAcknowledgement struct {
	Replica             string `json:"replica"`
	AcknowledgedThrough uint64 `json:"acknowledged_through"`
}

// AcknowledgeReplicaThrough records a monotone durable-journal sequence for a
// replica. A new replica consumes one slot from ReplicaRetentionCapacity.
func (journal *CommandJournal) AcknowledgeReplicaThrough(replica string, sequence uint64) error {
	if journal == nil {
		return ErrNilCommandJournal
	}
	replica = strings.TrimSpace(replica)
	if replica == "" || len(replica) > maxReplicaRetentionReplicaBytes {
		return ErrReplicaRetentionReplicaRequired
	}

	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.replicaRetentionCapacity <= 0 {
		return ErrReplicaRetentionDisabled
	}
	if sequence > journal.lastSequenceLocked() {
		return ErrReplicaRetentionSequenceInvalid
	}
	if journal.replicaRetentionAcks == nil {
		journal.replicaRetentionAcks = make(map[string]uint64, journal.replicaRetentionCapacity)
	}
	previous, exists := journal.replicaRetentionAcks[replica]
	if exists {
		if sequence < previous {
			return ErrReplicaRetentionAckRegressed
		}
		journal.replicaRetentionAcks[replica] = sequence
		return nil
	}
	if len(journal.replicaRetentionAcks) >= journal.replicaRetentionCapacity {
		return ErrReplicaRetentionCapacityExceeded
	}
	journal.replicaRetentionAcks[replica] = sequence
	journal.replicaRetentionInitialized = true
	return nil
}

// RemoveReplicaRetention removes a replica from the retention floor. The next
// prune can advance using the remaining replicas.
func (journal *CommandJournal) RemoveReplicaRetention(replica string) bool {
	if journal == nil {
		return false
	}
	replica = strings.TrimSpace(replica)
	if replica == "" {
		return false
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if _, exists := journal.replicaRetentionAcks[replica]; !exists {
		return false
	}
	delete(journal.replicaRetentionAcks, replica)
	return true
}

// ReplicaRetentionSnapshot returns sorted, detached runtime acknowledgements.
func (journal *CommandJournal) ReplicaRetentionSnapshot() []ReplicaRetentionAcknowledgement {
	if journal == nil {
		return nil
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if len(journal.replicaRetentionAcks) == 0 {
		return nil
	}
	snapshot := make([]ReplicaRetentionAcknowledgement, 0, len(journal.replicaRetentionAcks))
	for replica, sequence := range journal.replicaRetentionAcks {
		snapshot = append(snapshot, ReplicaRetentionAcknowledgement{
			Replica:             replica,
			AcknowledgedThrough: sequence,
		})
	}
	sort.Slice(snapshot, func(left, right int) bool {
		return snapshot[left].Replica < snapshot[right].Replica
	})
	return snapshot
}

func (journal *CommandJournal) replicaRetentionThroughLocked() (uint64, bool) {
	if journal == nil || journal.replicaRetentionCapacity <= 0 {
		return 0, false
	}
	if !journal.replicaRetentionInitialized {
		return 0, true
	}
	if len(journal.replicaRetentionAcks) == 0 {
		return 0, false
	}
	var through uint64
	found := false
	for _, sequence := range journal.replicaRetentionAcks {
		if !found || sequence < through {
			through = sequence
			found = true
		}
	}
	return through, found
}
