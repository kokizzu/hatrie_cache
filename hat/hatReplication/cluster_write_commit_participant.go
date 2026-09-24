package hatReplication

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

const (
	// DefaultClusterWriteCommitParticipantMaxRecords bounds retained
	// reconciliation state when callers do not provide a limit.
	DefaultClusterWriteCommitParticipantMaxRecords = 1024
	MaxClusterWriteCommitParticipantRecords        = 1 << 20
	MaxClusterWriteCommitParticipantSnapshotBytes  = 64 << 20
	maxClusterWriteCommitParticipantTransactionID  = 1 << 20
)

var (
	ErrClusterWriteCommitParticipantInvalid         = errors.New("hatReplication: invalid participant state")
	ErrClusterWriteCommitParticipantConflict        = errors.New("hatReplication: participant proposal conflicts with existing transaction")
	ErrClusterWriteCommitParticipantNotPrepared     = errors.New("hatReplication: participant transaction is not prepared")
	ErrClusterWriteCommitParticipantCommitted       = errors.New("hatReplication: participant transaction is already committed")
	ErrClusterWriteCommitParticipantAborted         = errors.New("hatReplication: participant transaction is already aborted")
	ErrClusterWriteCommitParticipantCapacity        = errors.New("hatReplication: participant state capacity exceeded")
	ErrClusterWriteCommitParticipantSnapshotInvalid = errors.New("hatReplication: participant snapshot is invalid")
)

// ClusterWriteCommitParticipantPhase is the durable local phase of one
// transaction. Committed and aborted are terminal states.
type ClusterWriteCommitParticipantPhase uint8

const (
	ClusterWriteCommitParticipantPrepared  ClusterWriteCommitParticipantPhase = 1
	ClusterWriteCommitParticipantCommitted ClusterWriteCommitParticipantPhase = 2
	ClusterWriteCommitParticipantAborted   ClusterWriteCommitParticipantPhase = 3
)

// ClusterWriteCommitParticipantRecord is the reconciliable local state for a
// transaction proposal.
type ClusterWriteCommitParticipantRecord struct {
	Proposal ClusterWriteCommitProposal         `json:"proposal"`
	Phase    ClusterWriteCommitParticipantPhase `json:"phase"`
}

// ClusterWriteCommitParticipantOptions bounds retained transaction state.
// MaxRecords == 0 selects DefaultClusterWriteCommitParticipantMaxRecords.
type ClusterWriteCommitParticipantOptions struct {
	MaxRecords int
}

// ClusterWriteCommitParticipant stores idempotent prepare/commit/abort state
// for one participant. It is transport-neutral; callers persist
// MarshalSnapshot output with their existing journal or snapshot mechanism.
type ClusterWriteCommitParticipant struct {
	mu         sync.RWMutex
	maxRecords int
	records    map[string]ClusterWriteCommitParticipantRecord
}

// NewClusterWriteCommitParticipant creates a bounded participant state store.
func NewClusterWriteCommitParticipant(options ClusterWriteCommitParticipantOptions) (*ClusterWriteCommitParticipant, error) {
	maxRecords := options.MaxRecords
	if maxRecords == 0 {
		maxRecords = DefaultClusterWriteCommitParticipantMaxRecords
	}
	if maxRecords < 1 || maxRecords > MaxClusterWriteCommitParticipantRecords {
		return nil, fmt.Errorf("%w: max records %d", ErrClusterWriteCommitParticipantInvalid, maxRecords)
	}
	return &ClusterWriteCommitParticipant{
		maxRecords: maxRecords,
		records:    make(map[string]ClusterWriteCommitParticipantRecord, min(maxRecords, 64)),
	}, nil
}

// Prepare records a proposal without making its write visible. Repeating the
// same proposal is idempotent, including after commit.
func (participant *ClusterWriteCommitParticipant) Prepare(proposal ClusterWriteCommitProposal) (ClusterWriteCommitParticipantRecord, error) {
	proposal, err := normalizeClusterWriteCommitParticipantProposal(proposal)
	if err != nil {
		return ClusterWriteCommitParticipantRecord{}, err
	}
	if participant == nil {
		return ClusterWriteCommitParticipantRecord{}, ErrClusterWriteCommitParticipantInvalid
	}
	participant.mu.Lock()
	defer participant.mu.Unlock()
	if existing, ok := participant.records[proposal.TransactionID]; ok {
		if existing.Proposal != proposal {
			return existing, ErrClusterWriteCommitParticipantConflict
		}
		if existing.Phase == ClusterWriteCommitParticipantAborted {
			return existing, ErrClusterWriteCommitParticipantAborted
		}
		return existing, nil
	}
	if len(participant.records) >= participant.maxRecords {
		return ClusterWriteCommitParticipantRecord{}, ErrClusterWriteCommitParticipantCapacity
	}
	record := ClusterWriteCommitParticipantRecord{Proposal: proposal, Phase: ClusterWriteCommitParticipantPrepared}
	participant.records[proposal.TransactionID] = record
	return record, nil
}

// Commit makes a prepared proposal visible. Repeating commit is idempotent.
func (participant *ClusterWriteCommitParticipant) Commit(proposal ClusterWriteCommitProposal) (ClusterWriteCommitParticipantRecord, error) {
	proposal, err := normalizeClusterWriteCommitParticipantProposal(proposal)
	if err != nil {
		return ClusterWriteCommitParticipantRecord{}, err
	}
	if participant == nil {
		return ClusterWriteCommitParticipantRecord{}, ErrClusterWriteCommitParticipantInvalid
	}
	participant.mu.Lock()
	defer participant.mu.Unlock()
	existing, ok := participant.records[proposal.TransactionID]
	if !ok {
		return ClusterWriteCommitParticipantRecord{}, ErrClusterWriteCommitParticipantNotPrepared
	}
	if existing.Proposal != proposal {
		return existing, ErrClusterWriteCommitParticipantConflict
	}
	switch existing.Phase {
	case ClusterWriteCommitParticipantCommitted:
		return existing, nil
	case ClusterWriteCommitParticipantAborted:
		return existing, ErrClusterWriteCommitParticipantAborted
	case ClusterWriteCommitParticipantPrepared:
		existing.Phase = ClusterWriteCommitParticipantCommitted
		participant.records[proposal.TransactionID] = existing
		return existing, nil
	default:
		return existing, ErrClusterWriteCommitParticipantInvalid
	}
}

// Abort releases a prepared proposal. Repeating abort is idempotent.
func (participant *ClusterWriteCommitParticipant) Abort(proposal ClusterWriteCommitProposal) (ClusterWriteCommitParticipantRecord, error) {
	proposal, err := normalizeClusterWriteCommitParticipantProposal(proposal)
	if err != nil {
		return ClusterWriteCommitParticipantRecord{}, err
	}
	if participant == nil {
		return ClusterWriteCommitParticipantRecord{}, ErrClusterWriteCommitParticipantInvalid
	}
	participant.mu.Lock()
	defer participant.mu.Unlock()
	existing, ok := participant.records[proposal.TransactionID]
	if !ok {
		return ClusterWriteCommitParticipantRecord{}, ErrClusterWriteCommitParticipantNotPrepared
	}
	if existing.Proposal != proposal {
		return existing, ErrClusterWriteCommitParticipantConflict
	}
	switch existing.Phase {
	case ClusterWriteCommitParticipantAborted:
		return existing, nil
	case ClusterWriteCommitParticipantCommitted:
		return existing, ErrClusterWriteCommitParticipantCommitted
	case ClusterWriteCommitParticipantPrepared:
		existing.Phase = ClusterWriteCommitParticipantAborted
		participant.records[proposal.TransactionID] = existing
		return existing, nil
	default:
		return existing, ErrClusterWriteCommitParticipantInvalid
	}
}

// Status returns a copy of one transaction's local phase for reconciliation.
func (participant *ClusterWriteCommitParticipant) Status(transactionID string) (ClusterWriteCommitParticipantRecord, bool) {
	if participant == nil {
		return ClusterWriteCommitParticipantRecord{}, false
	}
	transactionID = strings.TrimSpace(transactionID)
	if transactionID == "" {
		return ClusterWriteCommitParticipantRecord{}, false
	}
	participant.mu.RLock()
	defer participant.mu.RUnlock()
	record, ok := participant.records[transactionID]
	return record, ok
}

// Count returns the number of retained transaction records.
func (participant *ClusterWriteCommitParticipant) Count() int {
	if participant == nil {
		return 0
	}
	participant.mu.RLock()
	defer participant.mu.RUnlock()
	return len(participant.records)
}

// MarshalSnapshot encodes the participant state as a deterministic HCP1
// binary snapshot. Records are sorted by transaction ID and contain no map
// iteration order or timestamp entropy.
func (participant *ClusterWriteCommitParticipant) MarshalSnapshot() ([]byte, error) {
	if participant == nil {
		return nil, ErrClusterWriteCommitParticipantInvalid
	}
	participant.mu.RLock()
	records := make([]ClusterWriteCommitParticipantRecord, 0, len(participant.records))
	for _, record := range participant.records {
		records = append(records, record)
	}
	participant.mu.RUnlock()
	sortClusterWriteCommitParticipantRecords(records)
	return marshalClusterWriteCommitParticipantSnapshot(records)
}

// RestoreSnapshot atomically replaces the participant state after validating
// every record and the complete byte stream.
func (participant *ClusterWriteCommitParticipant) RestoreSnapshot(payload []byte) error {
	if participant == nil {
		return ErrClusterWriteCommitParticipantInvalid
	}
	participant.mu.RLock()
	maxRecords := participant.maxRecords
	participant.mu.RUnlock()
	records, err := unmarshalClusterWriteCommitParticipantSnapshot(payload, maxRecords)
	if err != nil {
		return err
	}
	participant.mu.Lock()
	participant.records = records
	participant.mu.Unlock()
	return nil
}

func normalizeClusterWriteCommitParticipantProposal(proposal ClusterWriteCommitProposal) (ClusterWriteCommitProposal, error) {
	proposal.TransactionID = strings.TrimSpace(proposal.TransactionID)
	if proposal.TransactionID == "" || len(proposal.TransactionID) > maxClusterWriteCommitParticipantTransactionID {
		return ClusterWriteCommitProposal{}, ErrClusterWriteCommitParticipantInvalid
	}
	return proposal, nil
}

func sortClusterWriteCommitParticipantRecords(records []ClusterWriteCommitParticipantRecord) {
	sort.Slice(records, func(left, right int) bool {
		return records[left].Proposal.TransactionID < records[right].Proposal.TransactionID
	})
}

const clusterWriteCommitParticipantSnapshotMagic = "HCP1"

func marshalClusterWriteCommitParticipantSnapshot(records []ClusterWriteCommitParticipantRecord) ([]byte, error) {
	payload := make([]byte, 0, len(clusterWriteCommitParticipantSnapshotMagic)+binary.MaxVarintLen64+len(records)*64)
	payload = append(payload, clusterWriteCommitParticipantSnapshotMagic...)
	payload = appendClusterWriteCommitParticipantUvarint(payload, uint64(len(records)))
	for _, record := range records {
		if record.Proposal.TransactionID == "" || len(record.Proposal.TransactionID) > maxClusterWriteCommitParticipantTransactionID || !validClusterWriteCommitParticipantPhase(record.Phase) {
			return nil, ErrClusterWriteCommitParticipantSnapshotInvalid
		}
		payload = appendClusterWriteCommitParticipantString(payload, record.Proposal.TransactionID)
		payload = appendClusterWriteCommitParticipantUvarint(payload, record.Proposal.Sequence)
		payload = appendClusterWriteCommitParticipantUvarint(payload, record.Proposal.FenceToken)
		payload = append(payload, record.Proposal.PayloadDigest[:]...)
		payload = append(payload, byte(record.Phase))
		if len(payload) > MaxClusterWriteCommitParticipantSnapshotBytes {
			return nil, ErrClusterWriteCommitParticipantSnapshotInvalid
		}
	}
	return payload, nil
}

func unmarshalClusterWriteCommitParticipantSnapshot(payload []byte, maxRecords int) (map[string]ClusterWriteCommitParticipantRecord, error) {
	if len(payload) < len(clusterWriteCommitParticipantSnapshotMagic) || len(payload) > MaxClusterWriteCommitParticipantSnapshotBytes || string(payload[:len(clusterWriteCommitParticipantSnapshotMagic)]) != clusterWriteCommitParticipantSnapshotMagic {
		return nil, ErrClusterWriteCommitParticipantSnapshotInvalid
	}
	offset := len(clusterWriteCommitParticipantSnapshotMagic)
	count, ok := readClusterWriteCommitParticipantUvarint(payload, &offset)
	if !ok || count > uint64(maxRecords) || count > MaxClusterWriteCommitParticipantRecords {
		return nil, ErrClusterWriteCommitParticipantSnapshotInvalid
	}
	records := make(map[string]ClusterWriteCommitParticipantRecord, int(count))
	for index := uint64(0); index < count; index++ {
		transactionID, ok := readClusterWriteCommitParticipantString(payload, &offset)
		if !ok || transactionID == "" || len(transactionID) > maxClusterWriteCommitParticipantTransactionID {
			return nil, ErrClusterWriteCommitParticipantSnapshotInvalid
		}
		if _, exists := records[transactionID]; exists {
			return nil, ErrClusterWriteCommitParticipantSnapshotInvalid
		}
		sequence, ok := readClusterWriteCommitParticipantUvarint(payload, &offset)
		if !ok {
			return nil, ErrClusterWriteCommitParticipantSnapshotInvalid
		}
		fenceToken, ok := readClusterWriteCommitParticipantUvarint(payload, &offset)
		if !ok || len(payload)-offset < 33 {
			return nil, ErrClusterWriteCommitParticipantSnapshotInvalid
		}
		var digest [32]byte
		copy(digest[:], payload[offset:offset+32])
		offset += 32
		phase := ClusterWriteCommitParticipantPhase(payload[offset])
		offset++
		if !validClusterWriteCommitParticipantPhase(phase) {
			return nil, ErrClusterWriteCommitParticipantSnapshotInvalid
		}
		records[transactionID] = ClusterWriteCommitParticipantRecord{
			Proposal: ClusterWriteCommitProposal{
				TransactionID: transactionID,
				Sequence:      sequence,
				FenceToken:    fenceToken,
				PayloadDigest: digest,
			},
			Phase: phase,
		}
	}
	if offset != len(payload) {
		return nil, ErrClusterWriteCommitParticipantSnapshotInvalid
	}
	return records, nil
}

func validClusterWriteCommitParticipantPhase(phase ClusterWriteCommitParticipantPhase) bool {
	return phase >= ClusterWriteCommitParticipantPrepared && phase <= ClusterWriteCommitParticipantAborted
}

func appendClusterWriteCommitParticipantUvarint(payload []byte, value uint64) []byte {
	var buffer [binary.MaxVarintLen64]byte
	return append(payload, buffer[:binary.PutUvarint(buffer[:], value)]...)
}

func appendClusterWriteCommitParticipantString(payload []byte, value string) []byte {
	payload = appendClusterWriteCommitParticipantUvarint(payload, uint64(len(value)))
	return append(payload, value...)
}

func readClusterWriteCommitParticipantUvarint(payload []byte, offset *int) (uint64, bool) {
	if *offset >= len(payload) {
		return 0, false
	}
	value, size := binary.Uvarint(payload[*offset:])
	if size <= 0 {
		return 0, false
	}
	*offset += size
	return value, true
}

func readClusterWriteCommitParticipantString(payload []byte, offset *int) (string, bool) {
	length, ok := readClusterWriteCommitParticipantUvarint(payload, offset)
	if !ok || length > uint64(len(payload)-*offset) || length > maxClusterWriteCommitParticipantTransactionID {
		return "", false
	}
	start := *offset
	*offset += int(length)
	return string(payload[start:*offset]), true
}
