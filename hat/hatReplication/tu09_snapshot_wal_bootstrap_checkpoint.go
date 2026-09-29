package hatReplication

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
)

const (
	// MaxSnapshotWALBootstrapSnapshotBytes bounds one persisted join checkpoint.
	MaxSnapshotWALBootstrapSnapshotBytes = 4096
	snapshotWALBootstrapSnapshotVersion  = 1
	snapshotWALBootstrapSnapshotHeader   = 4 + 1 + 4 + 4
)

var (
	// ErrSnapshotWALBootstrapSnapshotInvalid indicates a malformed checkpoint.
	ErrSnapshotWALBootstrapSnapshotInvalid = errors.New("hatReplication: snapshot WAL bootstrap checkpoint is invalid")
	// ErrSnapshotWALBootstrapSnapshotChecksum indicates corrupted checkpoint data.
	ErrSnapshotWALBootstrapSnapshotChecksum = errors.New("hatReplication: snapshot WAL bootstrap checkpoint checksum mismatch")
	// ErrSnapshotWALBootstrapPersist indicates checkpoint file I/O failure.
	ErrSnapshotWALBootstrapPersist = errors.New("hatReplication: snapshot WAL bootstrap checkpoint persistence failed")
)

var snapshotWALBootstrapSnapshotCRCTable = crc32.MakeTable(crc32.Castagnoli)

const snapshotWALBootstrapSnapshotMagic = "HSB1"

// MarshalSnapshot encodes the current join state as a deterministic, bounded,
// CRC-protected binary checkpoint. WAL records and filesystem paths are never
// included; callers can persist the returned bytes in their existing journal.
func (coordinator *SnapshotWALBootstrapCoordinator) MarshalSnapshot() ([]byte, error) {
	if coordinator == nil {
		return nil, ErrSnapshotWALBootstrapNil
	}
	coordinator.mu.RLock()
	state := coordinator.state
	maxWALGap := coordinator.maxWALGap
	coordinator.mu.RUnlock()
	if err := validateSnapshotWALBootstrapState(state, maxWALGap); err != nil {
		return nil, err
	}
	payload, err := marshalSnapshotWALBootstrapState(state)
	if err != nil {
		return nil, err
	}
	if len(payload) > MaxSnapshotWALBootstrapSnapshotBytes-snapshotWALBootstrapSnapshotHeader {
		return nil, ErrSnapshotWALBootstrapSnapshotInvalid
	}
	encoded := make([]byte, snapshotWALBootstrapSnapshotHeader+len(payload))
	copy(encoded[:4], snapshotWALBootstrapSnapshotMagic)
	encoded[4] = snapshotWALBootstrapSnapshotVersion
	binary.BigEndian.PutUint32(encoded[5:9], uint32(len(payload)))
	binary.BigEndian.PutUint32(encoded[9:13], crc32.Checksum(payload, snapshotWALBootstrapSnapshotCRCTable))
	copy(encoded[snapshotWALBootstrapSnapshotHeader:], payload)
	return encoded, nil
}

// RestoreSnapshot validates and atomically installs one checkpoint. A
// coordinator that already started a join cannot be overwritten; construct a
// new coordinator when recovering a process after restart.
func (coordinator *SnapshotWALBootstrapCoordinator) RestoreSnapshot(encoded []byte) error {
	if coordinator == nil {
		return ErrSnapshotWALBootstrapNil
	}
	state, err := unmarshalSnapshotWALBootstrapState(encoded)
	if err != nil {
		return err
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.state.Phase != SnapshotWALBootstrapPhaseIdle {
		return ErrSnapshotWALBootstrapAlreadyStarted
	}
	if err := validateSnapshotWALBootstrapState(state, coordinator.maxWALGap); err != nil {
		return err
	}
	coordinator.state = state
	return nil
}

// Save atomically persists a checkpoint with mode 0600 and a synced parent
// directory. The path is caller-owned and is never serialized into the frame.
func (coordinator *SnapshotWALBootstrapCoordinator) Save(path string) error {
	if coordinator == nil {
		return ErrSnapshotWALBootstrapNil
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("%w: path is required", ErrSnapshotWALBootstrapPersist)
	}
	encoded, err := coordinator.MarshalSnapshot()
	if err != nil {
		return err
	}
	return persistSnapshotWALBootstrapCheckpoint(path, encoded)
}

// LoadSnapshotWALBootstrapCoordinator creates a coordinator from one atomic
// checkpoint. Options still control the maximum accepted snapshot-to-WAL gap.
func LoadSnapshotWALBootstrapCoordinator(path string, options SnapshotWALBootstrapOptions) (*SnapshotWALBootstrapCoordinator, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("%w: path is required", ErrSnapshotWALBootstrapPersist)
	}
	coordinator, err := NewSnapshotWALBootstrapCoordinator(options)
	if err != nil {
		return nil, err
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("%w: read: %v", ErrSnapshotWALBootstrapPersist, err)
	}
	if err := coordinator.RestoreSnapshot(encoded); err != nil {
		return nil, err
	}
	return coordinator, nil
}

func validateSnapshotWALBootstrapState(state SnapshotWALBootstrapState, maxWALGap uint64) error {
	if maxWALGap == 0 || maxWALGap > MaxSnapshotWALBootstrapMaxGap {
		return ErrSnapshotWALBootstrapSnapshotInvalid
	}
	switch state.Phase {
	case SnapshotWALBootstrapPhaseIdle:
		if state != (SnapshotWALBootstrapState{}) {
			return ErrSnapshotWALBootstrapSnapshotInvalid
		}
		return nil
	case SnapshotWALBootstrapPhaseSnapshotPending, SnapshotWALBootstrapPhaseCatchingUp, SnapshotWALBootstrapPhaseReady, SnapshotWALBootstrapPhaseActive, SnapshotWALBootstrapPhaseAborted:
		if state.Generation == 0 {
			return ErrSnapshotWALBootstrapSnapshotInvalid
		}
		normalizedPlan, err := normalizeSnapshotWALBootstrapPlan(state.Plan, maxWALGap)
		if err != nil || normalizedPlan != state.Plan {
			return ErrSnapshotWALBootstrapSnapshotInvalid
		}
	default:
		return ErrSnapshotWALBootstrapSnapshotInvalid
	}
	plan := state.Plan
	switch state.Phase {
	case SnapshotWALBootstrapPhaseSnapshotPending:
		if state.AppliedJournalSequence != 0 || state.AbortReason != "" {
			return ErrSnapshotWALBootstrapSnapshotInvalid
		}
	case SnapshotWALBootstrapPhaseCatchingUp:
		if state.AbortReason != "" || state.AppliedJournalSequence < plan.SnapshotJournalSequence || state.AppliedJournalSequence > plan.TargetJournalSequence {
			return ErrSnapshotWALBootstrapSnapshotInvalid
		}
	case SnapshotWALBootstrapPhaseReady, SnapshotWALBootstrapPhaseActive:
		if state.AbortReason != "" || state.AppliedJournalSequence != plan.TargetJournalSequence {
			return ErrSnapshotWALBootstrapSnapshotInvalid
		}
	case SnapshotWALBootstrapPhaseAborted:
		normalizedReason, err := normalizeSnapshotWALBootstrapReason(state.AbortReason)
		if err != nil || normalizedReason != state.AbortReason {
			return ErrSnapshotWALBootstrapSnapshotInvalid
		}
		if state.AppliedJournalSequence != 0 && (state.AppliedJournalSequence < plan.SnapshotJournalSequence || state.AppliedJournalSequence > plan.TargetJournalSequence) {
			return ErrSnapshotWALBootstrapSnapshotInvalid
		}
	}
	return nil
}

func marshalSnapshotWALBootstrapState(state SnapshotWALBootstrapState) ([]byte, error) {
	payload := make([]byte, 0, 256)
	payload = append(payload, byte(state.Phase))
	payload = appendSnapshotWALBootstrapUvarint(payload, state.Generation)
	payload = appendSnapshotWALBootstrapUvarint(payload, state.AppliedJournalSequence)
	payload = appendSnapshotWALBootstrapString(payload, state.Plan.JoinerID)
	payload = appendSnapshotWALBootstrapString(payload, state.Plan.SourceID)
	payload = appendSnapshotWALBootstrapString(payload, state.Plan.SnapshotID)
	payload = appendSnapshotWALBootstrapUvarint(payload, state.Plan.StorageGeneration)
	payload = appendSnapshotWALBootstrapUvarint(payload, state.Plan.SnapshotJournalSequence)
	payload = appendSnapshotWALBootstrapUvarint(payload, state.Plan.TargetJournalSequence)
	payload = appendSnapshotWALBootstrapUvarint(payload, state.Plan.FencingToken)
	payload = appendSnapshotWALBootstrapString(payload, state.AbortReason)
	return payload, nil
}

func unmarshalSnapshotWALBootstrapState(encoded []byte) (SnapshotWALBootstrapState, error) {
	if len(encoded) < snapshotWALBootstrapSnapshotHeader || len(encoded) > MaxSnapshotWALBootstrapSnapshotBytes || string(encoded[:4]) != snapshotWALBootstrapSnapshotMagic {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapSnapshotInvalid
	}
	if encoded[4] != snapshotWALBootstrapSnapshotVersion {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapSnapshotInvalid
	}
	payloadLength := int(binary.BigEndian.Uint32(encoded[5:9]))
	if payloadLength < 1 || snapshotWALBootstrapSnapshotHeader+payloadLength != len(encoded) {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapSnapshotInvalid
	}
	payload := encoded[snapshotWALBootstrapSnapshotHeader:]
	if binary.BigEndian.Uint32(encoded[9:13]) != crc32.Checksum(payload, snapshotWALBootstrapSnapshotCRCTable) {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapSnapshotChecksum
	}
	offset := 0
	if offset >= len(payload) {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapSnapshotInvalid
	}
	state := SnapshotWALBootstrapState{Phase: SnapshotWALBootstrapPhase(payload[offset])}
	offset++
	var ok bool
	if state.Generation, ok = readSnapshotWALBootstrapUvarint(payload, &offset); !ok {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapSnapshotInvalid
	}
	if state.AppliedJournalSequence, ok = readSnapshotWALBootstrapUvarint(payload, &offset); !ok {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapSnapshotInvalid
	}
	if state.Plan.JoinerID, ok = readSnapshotWALBootstrapString(payload, &offset, MaxSnapshotWALBootstrapIdentifierBytes); !ok {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapSnapshotInvalid
	}
	if state.Plan.SourceID, ok = readSnapshotWALBootstrapString(payload, &offset, MaxSnapshotWALBootstrapIdentifierBytes); !ok {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapSnapshotInvalid
	}
	if state.Plan.SnapshotID, ok = readSnapshotWALBootstrapString(payload, &offset, MaxSnapshotWALBootstrapIdentifierBytes); !ok {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapSnapshotInvalid
	}
	if state.Plan.StorageGeneration, ok = readSnapshotWALBootstrapUvarint(payload, &offset); !ok {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapSnapshotInvalid
	}
	if state.Plan.SnapshotJournalSequence, ok = readSnapshotWALBootstrapUvarint(payload, &offset); !ok {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapSnapshotInvalid
	}
	if state.Plan.TargetJournalSequence, ok = readSnapshotWALBootstrapUvarint(payload, &offset); !ok {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapSnapshotInvalid
	}
	if state.Plan.FencingToken, ok = readSnapshotWALBootstrapUvarint(payload, &offset); !ok {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapSnapshotInvalid
	}
	if state.AbortReason, ok = readSnapshotWALBootstrapString(payload, &offset, MaxSnapshotWALBootstrapReasonBytes); !ok || offset != len(payload) {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapSnapshotInvalid
	}
	return state, nil
}

func appendSnapshotWALBootstrapUvarint(payload []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	return append(payload, encoded[:binary.PutUvarint(encoded[:], value)]...)
}

func appendSnapshotWALBootstrapString(payload []byte, value string) []byte {
	payload = appendSnapshotWALBootstrapUvarint(payload, uint64(len(value)))
	return append(payload, value...)
}

func readSnapshotWALBootstrapUvarint(payload []byte, offset *int) (uint64, bool) {
	if *offset >= len(payload) {
		return 0, false
	}
	value, size := binary.Uvarint(payload[*offset:])
	if size <= 0 {
		return 0, false
	}
	var canonical [binary.MaxVarintLen64]byte
	if binary.PutUvarint(canonical[:], value) != size {
		return 0, false
	}
	*offset += size
	return value, true
}

func readSnapshotWALBootstrapString(payload []byte, offset *int, maxBytes int) (string, bool) {
	length, ok := readSnapshotWALBootstrapUvarint(payload, offset)
	if !ok || length > uint64(maxBytes) || length > uint64(len(payload)-*offset) {
		return "", false
	}
	start := *offset
	*offset += int(length)
	return string(payload[start:*offset]), true
}

func persistSnapshotWALBootstrapCheckpoint(path string, encoded []byte) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("%w: mkdir: %v", ErrSnapshotWALBootstrapPersist, err)
	}
	temporary, err := os.CreateTemp(directory, ".hatrie-bootstrap-*")
	if err != nil {
		return fmt.Errorf("%w: create: %v", ErrSnapshotWALBootstrapPersist, err)
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
		return fmt.Errorf("%w: chmod: %v", ErrSnapshotWALBootstrapPersist, err)
	}
	if _, err := temporary.Write(encoded); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("%w: write: %v", ErrSnapshotWALBootstrapPersist, err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("%w: sync: %v", ErrSnapshotWALBootstrapPersist, err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("%w: close: %v", ErrSnapshotWALBootstrapPersist, err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("%w: rename: %v", ErrSnapshotWALBootstrapPersist, err)
	}
	removeTemporary = false
	directoryFile, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("%w: open directory: %v", ErrSnapshotWALBootstrapPersist, err)
	}
	syncErr := directoryFile.Sync()
	closeErr := directoryFile.Close()
	if syncErr != nil || closeErr != nil {
		return fmt.Errorf("%w: sync directory: %v", ErrSnapshotWALBootstrapPersist, errors.Join(syncErr, closeErr))
	}
	return nil
}
