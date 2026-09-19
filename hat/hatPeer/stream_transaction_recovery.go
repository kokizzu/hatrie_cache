package hatPeer

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

var (
	// ErrStreamTransactionRecoveryOptionsInvalid indicates invalid recovery bounds.
	ErrStreamTransactionRecoveryOptionsInvalid = errors.New("hatPeer: stream transaction recovery options are invalid")
	// ErrStreamTransactionRecoveryCorrupt indicates a malformed or truncated snapshot.
	ErrStreamTransactionRecoveryCorrupt = errors.New("hatPeer: stream transaction recovery snapshot is corrupt")
	// ErrStreamTransactionInvalid indicates an invalid stream transaction identifier.
	ErrStreamTransactionInvalid = errors.New("hatPeer: stream transaction identifier is invalid")
	// ErrStreamTransactionCapacity indicates that the recovery bound is full.
	ErrStreamTransactionCapacity = errors.New("hatPeer: stream transaction recovery capacity reached")
	// ErrStreamTransactionNotFound indicates that a transaction is unknown.
	ErrStreamTransactionNotFound = errors.New("hatPeer: stream transaction is not found")
	// ErrStreamTransactionTerminal indicates that a transaction is already finished.
	ErrStreamTransactionTerminal = errors.New("hatPeer: stream transaction is already terminal")
	// ErrStreamTransactionPending indicates that a transaction cannot be forgotten yet.
	ErrStreamTransactionPending = errors.New("hatPeer: stream transaction is still pending")
	// ErrStreamTransactionPayloadTooLarge indicates an input over the configured bound.
	ErrStreamTransactionPayloadTooLarge = errors.New("hatPeer: stream transaction payload is too large")
	// ErrStreamTransactionOperationLimit indicates that the operation bound is full.
	ErrStreamTransactionOperationLimit = errors.New("hatPeer: stream transaction operation limit reached")
	// ErrStreamTransactionOperationSequence indicates a non-contiguous operation sequence.
	ErrStreamTransactionOperationSequence = errors.New("hatPeer: stream transaction operation sequence is invalid")
	// ErrStreamTransactionOperationConflict indicates a reused sequence with different data.
	ErrStreamTransactionOperationConflict = errors.New("hatPeer: stream transaction operation conflicts with the recorded fingerprint")
	// ErrStreamTransactionRecoverySyncModeInvalid indicates an unknown sync mode.
	ErrStreamTransactionRecoverySyncModeInvalid = errors.New("hatPeer: stream transaction recovery sync mode is invalid")
)

const (
	// DefaultStreamTransactionMaxTransactions bounds retained transaction records.
	DefaultStreamTransactionMaxTransactions = 1024
	// DefaultStreamTransactionMaxOperations bounds fingerprints per transaction.
	DefaultStreamTransactionMaxOperations = 64
	// DefaultStreamTransactionMaxPayloadBytes bounds each command and payload.
	DefaultStreamTransactionMaxPayloadBytes = 1 << 20

	maxStreamTransactionMaxTransactions = 1 << 16
	maxStreamTransactionMaxOperations   = 1 << 12
	maxStreamTransactionMaxPayloadBytes = 16 << 20
	maxStreamTransactionRecoveryPath    = 4096
	maxStreamTransactionSnapshotBytes   = 64 << 20
	streamTransactionRecoveryVersion    = 1
)

var streamTransactionRecoveryMagic = [4]byte{'H', 'T', 'S', 'R'}

// StreamTransactionState is the durable lifecycle state of a stream transaction.
type StreamTransactionState uint8

const (
	StreamTransactionPending StreamTransactionState = iota + 1
	StreamTransactionCommitted
	StreamTransactionRolledBack
)

func (state StreamTransactionState) valid() bool {
	return state >= StreamTransactionPending && state <= StreamTransactionRolledBack
}

// StreamTransactionRecoverySyncMode controls when durable snapshots are synced.
type StreamTransactionRecoverySyncMode uint8

const (
	// StreamTransactionSyncOnCommit syncs terminal outcomes and keeps interim
	// begin/call snapshots low-cost. Pending calls are best-effort on a crash.
	StreamTransactionSyncOnCommit StreamTransactionRecoverySyncMode = iota + 1
	// StreamTransactionSyncEveryMutation syncs every begin, call, terminal, and
	// forget mutation for stronger pending-state recovery.
	StreamTransactionSyncEveryMutation
)

func (mode StreamTransactionRecoverySyncMode) valid() bool {
	return mode >= StreamTransactionSyncOnCommit && mode <= StreamTransactionSyncEveryMutation
}

// String returns the stable sync mode name.
func (mode StreamTransactionRecoverySyncMode) String() string {
	switch mode {
	case StreamTransactionSyncOnCommit:
		return "sync_on_commit"
	case StreamTransactionSyncEveryMutation:
		return "sync_every_mutation"
	default:
		return "unknown"
	}
}

// String returns the stable state name.
func (state StreamTransactionState) String() string {
	switch state {
	case StreamTransactionPending:
		return "pending"
	case StreamTransactionCommitted:
		return "committed"
	case StreamTransactionRolledBack:
		return "rolled_back"
	default:
		return "unknown"
	}
}

// StreamTransactionRecoveryOptions bounds and optionally persists recovery records.
// An empty Path keeps the ledger in memory; a configured Path uses private,
// checksummed, atomically replaced snapshots.
type StreamTransactionRecoveryOptions struct {
	Path            string
	MaxTransactions int
	MaxOperations   int
	MaxPayloadBytes int
	SyncMode        StreamTransactionRecoverySyncMode
}

// StreamTransactionOperation is a replay-safe fingerprint of one stream call.
// Raw commands and payloads are never persisted in the recovery snapshot.
type StreamTransactionOperation struct {
	Sequence    uint64
	Operation   CompactPeerStreamOperation
	CommandHash [sha256.Size]byte
	PayloadHash [sha256.Size]byte
}

// StreamTransactionSnapshot is an isolated copy of one recovery record.
type StreamTransactionSnapshot struct {
	StreamID   uint64
	State      StreamTransactionState
	Operations []StreamTransactionOperation
}

type streamTransactionRecord struct {
	state      StreamTransactionState
	operations []StreamTransactionOperation
}

// StreamTransactionRecovery is a bounded, concurrency-safe recovery ledger.
// It records transaction lifecycle and operation fingerprints; the caller still
// owns restoring storage-side transaction state in its stream handler.
type StreamTransactionRecovery struct {
	mu              sync.RWMutex
	path            string
	maxTransactions int
	maxOperations   int
	maxPayloadBytes int
	syncMode        StreamTransactionRecoverySyncMode
	transactions    map[uint64]*streamTransactionRecord
}

// NewStreamTransactionRecovery creates a memory-only recovery ledger.
func NewStreamTransactionRecovery(options StreamTransactionRecoveryOptions) (*StreamTransactionRecovery, error) {
	return newStreamTransactionRecovery(options)
}

// OpenStreamTransactionRecovery opens a recovery ledger and restores its snapshot when present.
func OpenStreamTransactionRecovery(options StreamTransactionRecoveryOptions) (*StreamTransactionRecovery, error) {
	recovery, err := newStreamTransactionRecovery(options)
	if err != nil {
		return nil, err
	}
	if recovery.path == "" {
		return recovery, nil
	}
	data, err := os.ReadFile(recovery.path)
	if errors.Is(err, os.ErrNotExist) {
		return recovery, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open stream transaction recovery: %w", err)
	}
	if len(data) > maxStreamTransactionSnapshotBytes {
		return nil, ErrStreamTransactionRecoveryCorrupt
	}
	transactions, err := decodeStreamTransactionSnapshot(data, recovery.maxTransactions, recovery.maxOperations)
	if err != nil {
		return nil, err
	}
	recovery.transactions = transactions
	return recovery, nil
}

func newStreamTransactionRecovery(options StreamTransactionRecoveryOptions) (*StreamTransactionRecovery, error) {
	maxTransactions := options.MaxTransactions
	if maxTransactions == 0 {
		maxTransactions = DefaultStreamTransactionMaxTransactions
	}
	maxOperations := options.MaxOperations
	if maxOperations == 0 {
		maxOperations = DefaultStreamTransactionMaxOperations
	}
	maxPayloadBytes := options.MaxPayloadBytes
	if maxPayloadBytes == 0 {
		maxPayloadBytes = DefaultStreamTransactionMaxPayloadBytes
	}
	syncMode := options.SyncMode
	if syncMode == 0 {
		syncMode = StreamTransactionSyncOnCommit
	}
	if !syncMode.valid() {
		return nil, ErrStreamTransactionRecoverySyncModeInvalid
	}
	if maxTransactions < 1 || maxTransactions > maxStreamTransactionMaxTransactions ||
		maxOperations < 1 || maxOperations > maxStreamTransactionMaxOperations ||
		maxPayloadBytes < 1 || maxPayloadBytes > maxStreamTransactionMaxPayloadBytes ||
		len(options.Path) > maxStreamTransactionRecoveryPath {
		return nil, ErrStreamTransactionRecoveryOptionsInvalid
	}
	return &StreamTransactionRecovery{
		path:            options.Path,
		maxTransactions: maxTransactions,
		maxOperations:   maxOperations,
		maxPayloadBytes: maxPayloadBytes,
		syncMode:        syncMode,
		transactions:    make(map[uint64]*streamTransactionRecord),
	}, nil
}

// Begin creates a pending transaction. Repeating Begin for a pending ID is idempotent.
func (recovery *StreamTransactionRecovery) Begin(streamID uint64) error {
	if recovery == nil || streamID == 0 {
		return ErrStreamTransactionInvalid
	}
	recovery.mu.Lock()
	defer recovery.mu.Unlock()
	if existing, ok := recovery.transactions[streamID]; ok {
		if existing.state == StreamTransactionPending {
			return nil
		}
		return ErrStreamTransactionTerminal
	}
	if len(recovery.transactions) >= recovery.maxTransactions {
		return ErrStreamTransactionCapacity
	}
	recovery.transactions[streamID] = &streamTransactionRecord{state: StreamTransactionPending}
	if recovery.syncMode != StreamTransactionSyncEveryMutation {
		return nil
	}
	if err := recovery.persistLocked(true); err != nil {
		delete(recovery.transactions, streamID)
		return err
	}
	return nil
}

// Record appends or idempotently replays one ordered stream call fingerprint.
func (recovery *StreamTransactionRecovery) Record(streamID, sequence uint64, operation CompactPeerStreamOperation, command, payload []byte) error {
	if recovery == nil || streamID == 0 || sequence == 0 {
		return ErrStreamTransactionInvalid
	}
	if operation != CompactPeerStreamCall {
		return ErrStreamTransactionOperationSequence
	}
	if len(command) == 0 || len(command) > recovery.maxPayloadBytes || len(payload) > recovery.maxPayloadBytes {
		return ErrStreamTransactionPayloadTooLarge
	}
	fingerprint := StreamTransactionOperation{
		Sequence:    sequence,
		Operation:   operation,
		CommandHash: sha256.Sum256(command),
		PayloadHash: sha256.Sum256(payload),
	}
	recovery.mu.Lock()
	defer recovery.mu.Unlock()
	record, ok := recovery.transactions[streamID]
	if !ok {
		return ErrStreamTransactionNotFound
	}
	if record.state != StreamTransactionPending {
		return ErrStreamTransactionTerminal
	}
	if sequence <= uint64(len(record.operations)) {
		if record.operations[sequence-1] == fingerprint {
			return nil
		}
		return ErrStreamTransactionOperationConflict
	}
	if sequence != uint64(len(record.operations))+1 {
		return ErrStreamTransactionOperationSequence
	}
	if len(record.operations) >= recovery.maxOperations {
		return ErrStreamTransactionOperationLimit
	}
	record.operations = append(record.operations, fingerprint)
	if recovery.syncMode != StreamTransactionSyncEveryMutation {
		return nil
	}
	if err := recovery.persistLocked(true); err != nil {
		record.operations = record.operations[:len(record.operations)-1]
		return err
	}
	return nil
}

// Commit marks a pending transaction committed. Repeating Commit is idempotent.
func (recovery *StreamTransactionRecovery) Commit(streamID uint64) error {
	return recovery.finish(streamID, StreamTransactionCommitted)
}

// Rollback marks a pending transaction rolled back. Repeating Rollback is idempotent.
func (recovery *StreamTransactionRecovery) Rollback(streamID uint64) error {
	return recovery.finish(streamID, StreamTransactionRolledBack)
}

func (recovery *StreamTransactionRecovery) finish(streamID uint64, state StreamTransactionState) error {
	if recovery == nil || streamID == 0 {
		return ErrStreamTransactionInvalid
	}
	recovery.mu.Lock()
	defer recovery.mu.Unlock()
	record, ok := recovery.transactions[streamID]
	if !ok {
		return ErrStreamTransactionNotFound
	}
	if record.state == state {
		return nil
	}
	if record.state != StreamTransactionPending {
		return ErrStreamTransactionTerminal
	}
	record.state = state
	if err := recovery.persistLocked(true); err != nil {
		record.state = StreamTransactionPending
		return err
	}
	return nil
}

// Forget removes a terminal record so bounded capacity can be reused.
func (recovery *StreamTransactionRecovery) Forget(streamID uint64) error {
	if recovery == nil || streamID == 0 {
		return ErrStreamTransactionInvalid
	}
	recovery.mu.Lock()
	defer recovery.mu.Unlock()
	record, ok := recovery.transactions[streamID]
	if !ok {
		return ErrStreamTransactionNotFound
	}
	if record.state == StreamTransactionPending {
		return ErrStreamTransactionPending
	}
	delete(recovery.transactions, streamID)
	if err := recovery.persistLocked(recovery.syncMode == StreamTransactionSyncEveryMutation); err != nil {
		recovery.transactions[streamID] = record
		return err
	}
	return nil
}

// Sync durably checkpoints the current ledger, including pending transactions.
// It is useful with StreamTransactionSyncOnCommit when pending recovery is
// required before a transaction reaches a terminal state.
func (recovery *StreamTransactionRecovery) Sync() error {
	if recovery == nil {
		return ErrStreamTransactionInvalid
	}
	recovery.mu.Lock()
	defer recovery.mu.Unlock()
	return recovery.persistLocked(true)
}

// Lookup returns an isolated transaction snapshot.
func (recovery *StreamTransactionRecovery) Lookup(streamID uint64) (StreamTransactionSnapshot, bool) {
	if recovery == nil || streamID == 0 {
		return StreamTransactionSnapshot{}, false
	}
	recovery.mu.RLock()
	defer recovery.mu.RUnlock()
	record, ok := recovery.transactions[streamID]
	if !ok {
		return StreamTransactionSnapshot{}, false
	}
	return cloneStreamTransactionSnapshot(streamID, record), true
}

// LookupInto copies a record into caller-owned storage. Supplying a large
// enough Operations buffer keeps repeated recovery checks allocation-free.
func (recovery *StreamTransactionRecovery) LookupInto(streamID uint64, destination *StreamTransactionSnapshot) bool {
	if recovery == nil || streamID == 0 || destination == nil {
		return false
	}
	recovery.mu.RLock()
	defer recovery.mu.RUnlock()
	record, ok := recovery.transactions[streamID]
	if !ok {
		return false
	}
	destination.StreamID = streamID
	destination.State = record.state
	if cap(destination.Operations) < len(record.operations) {
		destination.Operations = make([]StreamTransactionOperation, len(record.operations))
	} else {
		destination.Operations = destination.Operations[:len(record.operations)]
	}
	copy(destination.Operations, record.operations)
	return true
}

// Snapshot returns all records in stream-ID order.
func (recovery *StreamTransactionRecovery) Snapshot() []StreamTransactionSnapshot {
	if recovery == nil {
		return nil
	}
	recovery.mu.RLock()
	defer recovery.mu.RUnlock()
	ids := make([]uint64, 0, len(recovery.transactions))
	for streamID := range recovery.transactions {
		ids = append(ids, streamID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	result := make([]StreamTransactionSnapshot, 0, len(ids))
	for _, streamID := range ids {
		result = append(result, cloneStreamTransactionSnapshot(streamID, recovery.transactions[streamID]))
	}
	return result
}

// Pending returns only pending records in stream-ID order.
func (recovery *StreamTransactionRecovery) Pending() []StreamTransactionSnapshot {
	snapshots := recovery.Snapshot()
	result := snapshots[:0]
	for _, snapshot := range snapshots {
		if snapshot.State == StreamTransactionPending {
			result = append(result, snapshot)
		}
	}
	return result
}

func cloneStreamTransactionSnapshot(streamID uint64, record *streamTransactionRecord) StreamTransactionSnapshot {
	operations := make([]StreamTransactionOperation, len(record.operations))
	copy(operations, record.operations)
	return StreamTransactionSnapshot{StreamID: streamID, State: record.state, Operations: operations}
}

func (recovery *StreamTransactionRecovery) persistLocked(syncDurable bool) error {
	if recovery.path == "" {
		return nil
	}
	data, err := encodeStreamTransactionSnapshot(recovery.transactions)
	if err != nil {
		return err
	}
	directory := filepath.Dir(recovery.path)
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return fmt.Errorf("create stream transaction recovery directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".stream-recovery-*")
	if err != nil {
		return fmt.Errorf("create stream transaction recovery temporary file: %w", err)
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
		return fmt.Errorf("protect stream transaction recovery snapshot: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write stream transaction recovery snapshot: %w", err)
	}
	if syncDurable {
		if err := temporary.Sync(); err != nil {
			_ = temporary.Close()
			return fmt.Errorf("sync stream transaction recovery snapshot: %w", err)
		}
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close stream transaction recovery snapshot: %w", err)
	}
	if err := os.Rename(temporaryPath, recovery.path); err != nil {
		return fmt.Errorf("replace stream transaction recovery snapshot: %w", err)
	}
	removeTemporary = false
	if !syncDurable {
		return nil
	}
	directoryFile, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open stream transaction recovery directory: %w", err)
	}
	syncErr := directoryFile.Sync()
	closeErr := directoryFile.Close()
	if syncErr != nil {
		return fmt.Errorf("sync stream transaction recovery directory: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close stream transaction recovery directory: %w", closeErr)
	}
	return nil
}

func encodeStreamTransactionSnapshot(transactions map[uint64]*streamTransactionRecord) ([]byte, error) {
	ids := make([]uint64, 0, len(transactions))
	for streamID := range transactions {
		ids = append(ids, streamID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	var body bytes.Buffer
	body.Grow(9 + len(ids)*13)
	body.Write(streamTransactionRecoveryMagic[:])
	body.WriteByte(streamTransactionRecoveryVersion)
	if err := binary.Write(&body, binary.LittleEndian, uint32(len(ids))); err != nil {
		return nil, err
	}
	for _, streamID := range ids {
		record := transactions[streamID]
		if record == nil || !record.state.valid() || streamID == 0 {
			return nil, ErrStreamTransactionRecoveryCorrupt
		}
		if err := binary.Write(&body, binary.LittleEndian, streamID); err != nil {
			return nil, err
		}
		body.WriteByte(byte(record.state))
		if err := binary.Write(&body, binary.LittleEndian, uint32(len(record.operations))); err != nil {
			return nil, err
		}
		for index, operation := range record.operations {
			if operation.Sequence != uint64(index+1) || operation.Operation != CompactPeerStreamCall {
				return nil, ErrStreamTransactionRecoveryCorrupt
			}
			if err := binary.Write(&body, binary.LittleEndian, operation.Sequence); err != nil {
				return nil, err
			}
			body.WriteByte(byte(operation.Operation))
			body.Write(operation.CommandHash[:])
			body.Write(operation.PayloadHash[:])
		}
	}
	if body.Len()+4 > maxStreamTransactionSnapshotBytes {
		return nil, ErrStreamTransactionRecoveryCorrupt
	}
	checksum := crc32.Checksum(body.Bytes(), crc32.MakeTable(crc32.Castagnoli))
	if err := binary.Write(&body, binary.LittleEndian, checksum); err != nil {
		return nil, err
	}
	return body.Bytes(), nil
}

func decodeStreamTransactionSnapshot(data []byte, maxTransactions, maxOperations int) (map[uint64]*streamTransactionRecord, error) {
	if len(data) < 13 || len(data) > maxStreamTransactionSnapshotBytes {
		return nil, ErrStreamTransactionRecoveryCorrupt
	}
	storedChecksum := binary.LittleEndian.Uint32(data[len(data)-4:])
	actualChecksum := crc32.Checksum(data[:len(data)-4], crc32.MakeTable(crc32.Castagnoli))
	if storedChecksum != actualChecksum {
		return nil, ErrStreamTransactionRecoveryCorrupt
	}
	reader := bytes.NewReader(data[:len(data)-4])
	var magic [4]byte
	if _, err := io.ReadFull(reader, magic[:]); err != nil || magic != streamTransactionRecoveryMagic {
		return nil, ErrStreamTransactionRecoveryCorrupt
	}
	version, err := reader.ReadByte()
	if err != nil || version != streamTransactionRecoveryVersion {
		return nil, ErrStreamTransactionRecoveryCorrupt
	}
	var count uint32
	if err := binary.Read(reader, binary.LittleEndian, &count); err != nil || count > uint32(maxTransactions) {
		return nil, ErrStreamTransactionRecoveryCorrupt
	}
	transactions := make(map[uint64]*streamTransactionRecord, count)
	for index := uint32(0); index < count; index++ {
		var streamID uint64
		if err := binary.Read(reader, binary.LittleEndian, &streamID); err != nil || streamID == 0 {
			return nil, ErrStreamTransactionRecoveryCorrupt
		}
		if _, exists := transactions[streamID]; exists {
			return nil, ErrStreamTransactionRecoveryCorrupt
		}
		state, err := reader.ReadByte()
		if err != nil || !StreamTransactionState(state).valid() {
			return nil, ErrStreamTransactionRecoveryCorrupt
		}
		var operationCount uint32
		if err := binary.Read(reader, binary.LittleEndian, &operationCount); err != nil || operationCount > uint32(maxOperations) {
			return nil, ErrStreamTransactionRecoveryCorrupt
		}
		record := &streamTransactionRecord{state: StreamTransactionState(state), operations: make([]StreamTransactionOperation, operationCount)}
		for operationIndex := uint32(0); operationIndex < operationCount; operationIndex++ {
			var sequence uint64
			if err := binary.Read(reader, binary.LittleEndian, &sequence); err != nil || sequence != uint64(operationIndex+1) {
				return nil, ErrStreamTransactionRecoveryCorrupt
			}
			operation, err := reader.ReadByte()
			if err != nil || CompactPeerStreamOperation(operation) != CompactPeerStreamCall {
				return nil, ErrStreamTransactionRecoveryCorrupt
			}
			record.operations[operationIndex].Sequence = sequence
			record.operations[operationIndex].Operation = CompactPeerStreamOperation(operation)
			if _, err := io.ReadFull(reader, record.operations[operationIndex].CommandHash[:]); err != nil {
				return nil, ErrStreamTransactionRecoveryCorrupt
			}
			if _, err := io.ReadFull(reader, record.operations[operationIndex].PayloadHash[:]); err != nil {
				return nil, ErrStreamTransactionRecoveryCorrupt
			}
		}
		transactions[streamID] = record
	}
	if reader.Len() != 0 {
		return nil, ErrStreamTransactionRecoveryCorrupt
	}
	return transactions, nil
}
