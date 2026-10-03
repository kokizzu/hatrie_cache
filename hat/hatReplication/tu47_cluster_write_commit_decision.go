package hatReplication

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const (
	// DefaultClusterWriteCommitDecisionMaxRecords bounds retained coordinator
	// outcomes when the file store does not receive an explicit limit.
	DefaultClusterWriteCommitDecisionMaxRecords = 1024
	// MaxClusterWriteCommitDecisionRecords bounds decoded coordinator outcomes.
	MaxClusterWriteCommitDecisionRecords = 1 << 20
	// MaxClusterWriteCommitDecisionSnapshotBytes bounds one encoded decision
	// snapshot before its file envelope is added.
	MaxClusterWriteCommitDecisionSnapshotBytes = 64 << 20
	maxClusterWriteCommitDecisionTransactionID = 1 << 20
	maxClusterWriteCommitDecisionNodeBytes     = 1 << 16
)

var (
	// ErrClusterWriteCommitDecisionInvalid reports an invalid decision record
	// or recorder configuration.
	ErrClusterWriteCommitDecisionInvalid = errors.New("hatReplication: cluster write commit decision is invalid")
	// ErrClusterWriteCommitDecisionConflict reports a transaction whose
	// proposal or participant set differs from its durable record.
	ErrClusterWriteCommitDecisionConflict = errors.New("hatReplication: cluster write commit decision conflicts with existing record")
	// ErrClusterWriteCommitDecisionTransition reports a non-monotone phase.
	ErrClusterWriteCommitDecisionTransition = errors.New("hatReplication: cluster write commit decision phase transition is invalid")
	// ErrClusterWriteCommitDecisionCapacity reports a full bounded decision log.
	ErrClusterWriteCommitDecisionCapacity = errors.New("hatReplication: cluster write commit decision capacity exceeded")
	// ErrClusterWriteCommitDecisionPersist wraps a recorder or file-system
	// failure. The caller must not assume a durable recovery record exists.
	ErrClusterWriteCommitDecisionPersist = errors.New("hatReplication: cluster write commit decision persistence failed")
	// ErrClusterWriteCommitDecisionSnapshotInvalid reports malformed or
	// non-canonical durable bytes.
	ErrClusterWriteCommitDecisionSnapshotInvalid = errors.New("hatReplication: cluster write commit decision snapshot is invalid")
	// ErrClusterWriteCommitDecisionChecksum reports a damaged file envelope.
	ErrClusterWriteCommitDecisionChecksum = errors.New("hatReplication: cluster write commit decision checksum mismatch")
	// ErrClusterWriteCommitDecisionContextInvalid reports a nil context.
	ErrClusterWriteCommitDecisionContextInvalid = errors.New("hatReplication: cluster write commit decision context is nil")
)

var clusterWriteCommitDecisionCRCTable = crc32.MakeTable(crc32.Castagnoli)

// ClusterWriteCommitDecisionPhase is the durable coordinator phase for one
// transaction. Terminal phases are Committed, Aborted, and Indeterminate.
type ClusterWriteCommitDecisionPhase uint8

const (
	ClusterWriteCommitDecisionPreparing     ClusterWriteCommitDecisionPhase = 1
	ClusterWriteCommitDecisionPrepared      ClusterWriteCommitDecisionPhase = 2
	ClusterWriteCommitDecisionCommitting    ClusterWriteCommitDecisionPhase = 3
	ClusterWriteCommitDecisionCommitted     ClusterWriteCommitDecisionPhase = 4
	ClusterWriteCommitDecisionAborted       ClusterWriteCommitDecisionPhase = 5
	ClusterWriteCommitDecisionIndeterminate ClusterWriteCommitDecisionPhase = 6
)

// ClusterWriteCommitDecision is a deterministic, restart-safe coordinator
// record. Nodes preserve the coordinator's normalized input order because that
// order is also used by the participant result and reconciliation callbacks.
type ClusterWriteCommitDecision struct {
	Proposal ClusterWriteCommitProposal      `json:"proposal"`
	Nodes    []string                        `json:"nodes"`
	Phase    ClusterWriteCommitDecisionPhase `json:"phase"`
}

// ClusterWriteCommitDecisionRecorder persists phase transitions before the
// corresponding coordinator phase starts. Implementations must make Record
// idempotent for an identical decision.
type ClusterWriteCommitDecisionRecorder interface {
	Record(context.Context, ClusterWriteCommitDecision) error
}

// ClusterWriteCommitDecisionFileStoreOptions bounds one local durable
// coordinator decision file. The file store is intended for one coordinator
// owner per path; atomic replacement protects readers and crash recovery.
type ClusterWriteCommitDecisionFileStoreOptions struct {
	Path       string
	MaxRecords int
	MaxBytes   int
}

// ClusterWriteCommitDecisionFileStore is an opt-in durable decision recorder.
// Each transition is encoded as a bounded deterministic snapshot, written to a
// private temporary file, fsynced, atomically renamed, and followed by a
// directory sync. It does not alter the normal coordinator path.
type ClusterWriteCommitDecisionFileStore struct {
	mu         sync.Mutex
	path       string
	maxRecords int
	maxBytes   int
	loaded     bool
	records    map[string]ClusterWriteCommitDecision
}

// NewClusterWriteCommitDecisionFileStore creates a bounded local decision
// recorder. Missing files are treated as an empty log until the first record.
func NewClusterWriteCommitDecisionFileStore(options ClusterWriteCommitDecisionFileStoreOptions) (*ClusterWriteCommitDecisionFileStore, error) {
	path := strings.TrimSpace(options.Path)
	if path == "" {
		return nil, ErrClusterWriteCommitDecisionInvalid
	}
	maxRecords := options.MaxRecords
	if maxRecords == 0 {
		maxRecords = DefaultClusterWriteCommitDecisionMaxRecords
	}
	if maxRecords < 1 || maxRecords > MaxClusterWriteCommitDecisionRecords {
		return nil, fmt.Errorf("%w: max records %d", ErrClusterWriteCommitDecisionInvalid, maxRecords)
	}
	maxBytes := options.MaxBytes
	if maxBytes == 0 {
		maxBytes = MaxClusterWriteCommitDecisionSnapshotBytes
	}
	if maxBytes < decisionFileHeaderSize()+len(decisionSnapshotMagic)+1 || maxBytes > MaxClusterWriteCommitDecisionSnapshotBytes {
		return nil, fmt.Errorf("%w: max bytes %d", ErrClusterWriteCommitDecisionInvalid, maxBytes)
	}
	return &ClusterWriteCommitDecisionFileStore{
		path:       path,
		maxRecords: maxRecords,
		maxBytes:   maxBytes,
		records:    make(map[string]ClusterWriteCommitDecision),
	}, nil
}

// Record durably advances one decision. Repeating an identical phase is a
// no-op; conflicting proposals and phase regressions are rejected.
func (store *ClusterWriteCommitDecisionFileStore) Record(ctx context.Context, decision ClusterWriteCommitDecision) error {
	if store == nil {
		return ErrClusterWriteCommitDecisionInvalid
	}
	if err := validateClusterWriteCommitDecisionContext(ctx); err != nil {
		return err
	}
	decision, err := normalizeClusterWriteCommitDecision(decision)
	if err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ensureLoadedLocked(); err != nil {
		return errors.Join(ErrClusterWriteCommitDecisionPersist, err)
	}
	if existing, ok := store.records[decision.Proposal.TransactionID]; ok {
		if !sameClusterWriteCommitDecisionIdentity(existing, decision) {
			return ErrClusterWriteCommitDecisionConflict
		}
		if existing.Phase == decision.Phase {
			return nil
		}
		if !validClusterWriteCommitDecisionTransition(existing.Phase, decision.Phase) {
			return fmt.Errorf("%w: %d to %d", ErrClusterWriteCommitDecisionTransition, existing.Phase, decision.Phase)
		}
	} else if len(store.records) >= store.maxRecords {
		return ErrClusterWriteCommitDecisionCapacity
	}
	next := cloneClusterWriteCommitDecisionRecords(store.records)
	next[decision.Proposal.TransactionID] = decision
	encoded, err := marshalClusterWriteCommitDecisionFile(next, store.maxRecords, store.maxBytes)
	if err != nil {
		return errors.Join(ErrClusterWriteCommitDecisionPersist, err)
	}
	if err := persistClusterWriteCommitDecisionFile(store.path, encoded); err != nil {
		return errors.Join(ErrClusterWriteCommitDecisionPersist, err)
	}
	store.records = next
	return nil
}

// Load returns one independently owned decision record.
func (store *ClusterWriteCommitDecisionFileStore) Load(ctx context.Context, transactionID string) (ClusterWriteCommitDecision, bool, error) {
	if store == nil {
		return ClusterWriteCommitDecision{}, false, ErrClusterWriteCommitDecisionInvalid
	}
	if err := validateClusterWriteCommitDecisionContext(ctx); err != nil {
		return ClusterWriteCommitDecision{}, false, err
	}
	transactionID = strings.TrimSpace(transactionID)
	if transactionID == "" || len(transactionID) > maxClusterWriteCommitDecisionTransactionID {
		return ClusterWriteCommitDecision{}, false, ErrClusterWriteCommitDecisionInvalid
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ensureLoadedLocked(); err != nil {
		return ClusterWriteCommitDecision{}, false, errors.Join(ErrClusterWriteCommitDecisionPersist, err)
	}
	decision, ok := store.records[transactionID]
	if !ok {
		return ClusterWriteCommitDecision{}, false, nil
	}
	return cloneClusterWriteCommitDecision(decision), true, nil
}

// List returns all retained decisions in transaction-ID order.
func (store *ClusterWriteCommitDecisionFileStore) List(ctx context.Context) ([]ClusterWriteCommitDecision, error) {
	if store == nil {
		return nil, ErrClusterWriteCommitDecisionInvalid
	}
	if err := validateClusterWriteCommitDecisionContext(ctx); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ensureLoadedLocked(); err != nil {
		return nil, errors.Join(ErrClusterWriteCommitDecisionPersist, err)
	}
	decisions := make([]ClusterWriteCommitDecision, 0, len(store.records))
	for _, decision := range store.records {
		decisions = append(decisions, cloneClusterWriteCommitDecision(decision))
	}
	sort.Slice(decisions, func(left, right int) bool {
		return decisions[left].Proposal.TransactionID < decisions[right].Proposal.TransactionID
	})
	return decisions, nil
}

// Delete removes one retained terminal or indeterminate record. It is an
// explicit caller-owned retention decision; the coordinator never deletes
// evidence needed for recovery.
func (store *ClusterWriteCommitDecisionFileStore) Delete(ctx context.Context, transactionID string) error {
	if store == nil {
		return ErrClusterWriteCommitDecisionInvalid
	}
	if err := validateClusterWriteCommitDecisionContext(ctx); err != nil {
		return err
	}
	transactionID = strings.TrimSpace(transactionID)
	if transactionID == "" || len(transactionID) > maxClusterWriteCommitDecisionTransactionID {
		return ErrClusterWriteCommitDecisionInvalid
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ensureLoadedLocked(); err != nil {
		return errors.Join(ErrClusterWriteCommitDecisionPersist, err)
	}
	if _, ok := store.records[transactionID]; !ok {
		return nil
	}
	next := cloneClusterWriteCommitDecisionRecords(store.records)
	delete(next, transactionID)
	encoded, err := marshalClusterWriteCommitDecisionFile(next, store.maxRecords, store.maxBytes)
	if err != nil {
		return errors.Join(ErrClusterWriteCommitDecisionPersist, err)
	}
	if err := persistClusterWriteCommitDecisionFile(store.path, encoded); err != nil {
		return errors.Join(ErrClusterWriteCommitDecisionPersist, err)
	}
	store.records = next
	return nil
}

func (store *ClusterWriteCommitDecisionFileStore) ensureLoadedLocked() error {
	if store.loaded {
		return nil
	}
	info, err := os.Lstat(store.path)
	if errors.Is(err, os.ErrNotExist) {
		store.loaded = true
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return ErrClusterWriteCommitDecisionSnapshotInvalid
	}
	if info.Size() < 0 || info.Size() > int64(store.maxBytes) {
		return ErrClusterWriteCommitDecisionSnapshotInvalid
	}
	file, err := os.Open(store.path)
	if err != nil {
		return err
	}
	encoded, readErr := io.ReadAll(io.LimitReader(file, int64(store.maxBytes)+1))
	closeErr := file.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	if len(encoded) > store.maxBytes {
		return ErrClusterWriteCommitDecisionSnapshotInvalid
	}
	records, err := unmarshalClusterWriteCommitDecisionFile(encoded, store.maxRecords, store.maxBytes)
	if err != nil {
		return err
	}
	store.records = records
	store.loaded = true
	return nil
}

func normalizeClusterWriteCommitDecision(decision ClusterWriteCommitDecision) (ClusterWriteCommitDecision, error) {
	proposal, err := normalizeClusterWriteCommitDecisionProposal(decision.Proposal)
	if err != nil {
		return ClusterWriteCommitDecision{}, err
	}
	nodes, err := normalizeClusterWriteCommitNodes(decision.Nodes)
	if err != nil {
		return ClusterWriteCommitDecision{}, errors.Join(ErrClusterWriteCommitDecisionInvalid, err)
	}
	for _, node := range nodes {
		if len(node) > maxClusterWriteCommitDecisionNodeBytes {
			return ClusterWriteCommitDecision{}, ErrClusterWriteCommitDecisionInvalid
		}
	}
	if !validClusterWriteCommitDecisionPhase(decision.Phase) {
		return ClusterWriteCommitDecision{}, ErrClusterWriteCommitDecisionInvalid
	}
	decision.Proposal = proposal
	decision.Nodes = nodes
	return decision, nil
}

func normalizeClusterWriteCommitDecisionProposal(proposal ClusterWriteCommitProposal) (ClusterWriteCommitProposal, error) {
	proposal.TransactionID = strings.TrimSpace(proposal.TransactionID)
	if proposal.TransactionID == "" || len(proposal.TransactionID) > maxClusterWriteCommitDecisionTransactionID {
		return ClusterWriteCommitProposal{}, ErrClusterWriteCommitDecisionInvalid
	}
	return proposal, nil
}

func validClusterWriteCommitDecisionPhase(phase ClusterWriteCommitDecisionPhase) bool {
	return phase >= ClusterWriteCommitDecisionPreparing && phase <= ClusterWriteCommitDecisionIndeterminate
}

func validClusterWriteCommitDecisionTransition(previous, next ClusterWriteCommitDecisionPhase) bool {
	switch previous {
	case ClusterWriteCommitDecisionPreparing:
		return next == ClusterWriteCommitDecisionPrepared || next == ClusterWriteCommitDecisionAborted
	case ClusterWriteCommitDecisionPrepared:
		return next == ClusterWriteCommitDecisionCommitting || next == ClusterWriteCommitDecisionAborted
	case ClusterWriteCommitDecisionCommitting:
		return next == ClusterWriteCommitDecisionCommitted || next == ClusterWriteCommitDecisionIndeterminate
	default:
		return false
	}
}

func sameClusterWriteCommitDecisionIdentity(left, right ClusterWriteCommitDecision) bool {
	if left.Proposal != right.Proposal || len(left.Nodes) != len(right.Nodes) {
		return false
	}
	for index := range left.Nodes {
		if left.Nodes[index] != right.Nodes[index] {
			return false
		}
	}
	return true
}

func cloneClusterWriteCommitDecision(decision ClusterWriteCommitDecision) ClusterWriteCommitDecision {
	decision.Nodes = append([]string(nil), decision.Nodes...)
	return decision
}

func cloneClusterWriteCommitDecisionRecords(records map[string]ClusterWriteCommitDecision) map[string]ClusterWriteCommitDecision {
	cloned := make(map[string]ClusterWriteCommitDecision, len(records)+1)
	for transactionID, decision := range records {
		cloned[transactionID] = cloneClusterWriteCommitDecision(decision)
	}
	return cloned
}

func validateClusterWriteCommitDecisionContext(ctx context.Context) error {
	if ctx == nil {
		return ErrClusterWriteCommitDecisionContextInvalid
	}
	return ctx.Err()
}

const (
	decisionSnapshotMagic = "HCD1"
	decisionFileMagic     = "HCDS1"
)

func decisionFileHeaderSize() int {
	return len(decisionFileMagic) + 8 + 4
}

func marshalClusterWriteCommitDecisionFile(records map[string]ClusterWriteCommitDecision, maxRecords, maxBytes int) ([]byte, error) {
	decisions := make([]ClusterWriteCommitDecision, 0, len(records))
	for _, decision := range records {
		decisions = append(decisions, decision)
	}
	sort.Slice(decisions, func(left, right int) bool {
		return decisions[left].Proposal.TransactionID < decisions[right].Proposal.TransactionID
	})
	payload, err := marshalClusterWriteCommitDecisionSnapshot(decisions, maxRecords)
	if err != nil {
		return nil, err
	}
	encoded := make([]byte, len(decisionFileMagic)+8+len(payload)+4)
	offset := copy(encoded, decisionFileMagic)
	binary.BigEndian.PutUint64(encoded[offset:offset+8], uint64(len(payload)))
	offset += 8
	copy(encoded[offset:], payload)
	checksumOffset := offset + len(payload)
	binary.BigEndian.PutUint32(encoded[checksumOffset:], crc32.Checksum(encoded[:checksumOffset], clusterWriteCommitDecisionCRCTable))
	if len(encoded) > maxBytes {
		return nil, ErrClusterWriteCommitDecisionCapacity
	}
	return encoded, nil
}

func unmarshalClusterWriteCommitDecisionFile(encoded []byte, maxRecords, maxBytes int) (map[string]ClusterWriteCommitDecision, error) {
	if len(encoded) < decisionFileHeaderSize() || len(encoded) > maxBytes || !bytes.Equal(encoded[:len(decisionFileMagic)], []byte(decisionFileMagic)) {
		return nil, ErrClusterWriteCommitDecisionSnapshotInvalid
	}
	offset := len(decisionFileMagic)
	payloadLength := binary.BigEndian.Uint64(encoded[offset : offset+8])
	offset += 8
	if payloadLength > uint64(len(encoded)-offset-4) || int(payloadLength) != len(encoded)-offset-4 {
		return nil, ErrClusterWriteCommitDecisionSnapshotInvalid
	}
	checksumOffset := offset + int(payloadLength)
	if binary.BigEndian.Uint32(encoded[checksumOffset:]) != crc32.Checksum(encoded[:checksumOffset], clusterWriteCommitDecisionCRCTable) {
		return nil, ErrClusterWriteCommitDecisionChecksum
	}
	return unmarshalClusterWriteCommitDecisionSnapshot(encoded[offset:checksumOffset], maxRecords)
}

func marshalClusterWriteCommitDecisionSnapshot(decisions []ClusterWriteCommitDecision, maxRecords int) ([]byte, error) {
	if len(decisions) > maxRecords || len(decisions) > MaxClusterWriteCommitDecisionRecords {
		return nil, ErrClusterWriteCommitDecisionCapacity
	}
	payload := make([]byte, 0, len(decisionSnapshotMagic)+binary.MaxVarintLen64+len(decisions)*96)
	payload = append(payload, decisionSnapshotMagic...)
	payload = appendClusterWriteCommitDecisionUvarint(payload, uint64(len(decisions)))
	for _, decision := range decisions {
		normalized, err := normalizeClusterWriteCommitDecision(decision)
		if err != nil {
			return nil, err
		}
		payload = appendClusterWriteCommitDecisionString(payload, normalized.Proposal.TransactionID)
		payload = appendClusterWriteCommitDecisionUvarint(payload, normalized.Proposal.Sequence)
		payload = appendClusterWriteCommitDecisionUvarint(payload, normalized.Proposal.FenceToken)
		payload = append(payload, normalized.Proposal.PayloadDigest[:]...)
		payload = appendClusterWriteCommitDecisionUvarint(payload, uint64(len(normalized.Nodes)))
		for _, node := range normalized.Nodes {
			if len(node) > maxClusterWriteCommitDecisionNodeBytes {
				return nil, ErrClusterWriteCommitDecisionInvalid
			}
			payload = appendClusterWriteCommitDecisionString(payload, node)
		}
		payload = append(payload, byte(normalized.Phase))
		if len(payload) > MaxClusterWriteCommitDecisionSnapshotBytes {
			return nil, ErrClusterWriteCommitDecisionCapacity
		}
	}
	return payload, nil
}

func unmarshalClusterWriteCommitDecisionSnapshot(payload []byte, maxRecords int) (map[string]ClusterWriteCommitDecision, error) {
	if len(payload) < len(decisionSnapshotMagic) || len(payload) > MaxClusterWriteCommitDecisionSnapshotBytes || !bytes.Equal(payload[:len(decisionSnapshotMagic)], []byte(decisionSnapshotMagic)) {
		return nil, ErrClusterWriteCommitDecisionSnapshotInvalid
	}
	offset := len(decisionSnapshotMagic)
	count, ok := readClusterWriteCommitDecisionUvarint(payload, &offset)
	if !ok || count > uint64(maxRecords) || count > MaxClusterWriteCommitDecisionRecords {
		return nil, ErrClusterWriteCommitDecisionSnapshotInvalid
	}
	records := make(map[string]ClusterWriteCommitDecision, int(count))
	previousID := ""
	for index := uint64(0); index < count; index++ {
		transactionID, ok := readClusterWriteCommitDecisionString(payload, &offset, maxClusterWriteCommitDecisionTransactionID)
		if !ok || transactionID == "" || (index > 0 && transactionID <= previousID) {
			return nil, ErrClusterWriteCommitDecisionSnapshotInvalid
		}
		previousID = transactionID
		sequence, ok := readClusterWriteCommitDecisionUvarint(payload, &offset)
		if !ok {
			return nil, ErrClusterWriteCommitDecisionSnapshotInvalid
		}
		fenceToken, ok := readClusterWriteCommitDecisionUvarint(payload, &offset)
		if !ok || len(payload)-offset < 32 {
			return nil, ErrClusterWriteCommitDecisionSnapshotInvalid
		}
		var digest [32]byte
		copy(digest[:], payload[offset:offset+32])
		offset += 32
		nodeCount, ok := readClusterWriteCommitDecisionUvarint(payload, &offset)
		if !ok || nodeCount == 0 || nodeCount > MaxClusterWriteCommitNodes {
			return nil, ErrClusterWriteCommitDecisionSnapshotInvalid
		}
		nodes := make([]string, int(nodeCount))
		for nodeIndex := range nodes {
			node, ok := readClusterWriteCommitDecisionString(payload, &offset, maxClusterWriteCommitDecisionNodeBytes)
			if !ok {
				return nil, ErrClusterWriteCommitDecisionSnapshotInvalid
			}
			nodes[nodeIndex] = node
		}
		if offset >= len(payload) {
			return nil, ErrClusterWriteCommitDecisionSnapshotInvalid
		}
		decision := ClusterWriteCommitDecision{
			Proposal: ClusterWriteCommitProposal{TransactionID: transactionID, Sequence: sequence, FenceToken: fenceToken, PayloadDigest: digest},
			Nodes:    nodes,
			Phase:    ClusterWriteCommitDecisionPhase(payload[offset]),
		}
		offset++
		normalized, err := normalizeClusterWriteCommitDecision(decision)
		if err != nil {
			return nil, ErrClusterWriteCommitDecisionSnapshotInvalid
		}
		if _, exists := records[transactionID]; exists {
			return nil, ErrClusterWriteCommitDecisionSnapshotInvalid
		}
		records[transactionID] = normalized
	}
	if offset != len(payload) {
		return nil, ErrClusterWriteCommitDecisionSnapshotInvalid
	}
	return records, nil
}

func appendClusterWriteCommitDecisionUvarint(payload []byte, value uint64) []byte {
	var buffer [binary.MaxVarintLen64]byte
	return append(payload, buffer[:binary.PutUvarint(buffer[:], value)]...)
}

func appendClusterWriteCommitDecisionString(payload []byte, value string) []byte {
	payload = appendClusterWriteCommitDecisionUvarint(payload, uint64(len(value)))
	return append(payload, value...)
}

func readClusterWriteCommitDecisionUvarint(payload []byte, offset *int) (uint64, bool) {
	if *offset >= len(payload) {
		return 0, false
	}
	value, size := binary.Uvarint(payload[*offset:])
	if size <= 0 || size > binary.MaxVarintLen64 {
		return 0, false
	}
	var canonical [binary.MaxVarintLen64]byte
	if binary.PutUvarint(canonical[:], value) != size || !bytes.Equal(canonical[:size], payload[*offset:*offset+size]) {
		return 0, false
	}
	*offset += size
	return value, true
}

func readClusterWriteCommitDecisionString(payload []byte, offset *int, maxBytes int) (string, bool) {
	length, ok := readClusterWriteCommitDecisionUvarint(payload, offset)
	if !ok || length > uint64(maxBytes) || length > uint64(len(payload)-*offset) {
		return "", false
	}
	start := *offset
	*offset += int(length)
	return string(payload[start:*offset]), true
}

func persistClusterWriteCommitDecisionFile(path string, encoded []byte) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".hatrie-cluster-write-decision-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := writeClusterWriteCommitDecisionBytes(temporary, encoded); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	removeTemporary = false
	directoryFile, err := os.Open(directory)
	if err != nil {
		return err
	}
	syncErr := directoryFile.Sync()
	closeErr := directoryFile.Close()
	return errors.Join(syncErr, closeErr)
}

func writeClusterWriteCommitDecisionBytes(writer io.Writer, payload []byte) error {
	for len(payload) > 0 {
		written, err := writer.Write(payload)
		if err != nil {
			return err
		}
		if written <= 0 || written > len(payload) {
			return io.ErrShortWrite
		}
		payload = payload[written:]
	}
	return nil
}
