package hatReplication

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"sync"
	"unicode/utf8"
)

const (
	MaxExactlyOnceUpsertSinkIdentityBytes = 256
	MaxExactlyOnceUpsertSinkKeyBytes      = 1 << 20
	MaxExactlyOnceUpsertSinkValueBytes    = 1 << 20
	MaxExactlyOnceUpsertSinkSnapshotBytes = 4 << 20
	exactlyOnceUpsertSinkSnapshotVersion  = 1
	exactlyOnceUpsertSinkFlagCommitted    = 1 << 0
	exactlyOnceUpsertSinkFlagPending      = 1 << 1
)

var (
	ErrExactlyOnceUpsertSinkInvalid           = errors.New("hatriecache: exactly-once upsert sink state is invalid")
	ErrExactlyOnceUpsertSinkPending           = errors.New("hatriecache: exactly-once upsert sink has a pending record")
	ErrExactlyOnceUpsertSinkGap               = errors.New("hatriecache: exactly-once upsert sink sequence gap")
	ErrExactlyOnceUpsertSinkOverlap           = errors.New("hatriecache: exactly-once upsert sink sequence overlaps")
	ErrExactlyOnceUpsertSinkConflict          = errors.New("hatriecache: exactly-once upsert sink record conflicts")
	ErrExactlyOnceUpsertSinkNoPending         = errors.New("hatriecache: exactly-once upsert sink has no pending record")
	ErrExactlyOnceUpsertSinkSequenceExhausted = errors.New("hatriecache: exactly-once upsert sink sequence exhausted")
	ErrExactlyOnceUpsertSinkSnapshotInvalid   = errors.New("hatriecache: exactly-once upsert sink snapshot is invalid")
)

// ExactlyOnceUpsertSinkRecord is one ordered upsert or tombstone. OutputID is
// the durable identity of the destination row or object and may be reused by
// later updates to that same destination.
type ExactlyOnceUpsertSinkRecord struct {
	Sequence uint64 `json:"sequence"`
	OutputID string `json:"output_id"`
	Key      []byte `json:"key"`
	Value    []byte `json:"value,omitempty"`
	Delete   bool   `json:"delete,omitempty"`
}

// ExactlyOnceUpsertSinkAction tells the caller whether the external upsert
// should be applied, resumed after restart, or skipped as already committed.
type ExactlyOnceUpsertSinkAction uint8

const (
	ExactlyOnceUpsertSinkApply ExactlyOnceUpsertSinkAction = iota + 1
	ExactlyOnceUpsertSinkResume
	ExactlyOnceUpsertSinkSkip
)

// ExactlyOnceUpsertSinkDecision is the result of beginning one record.
type ExactlyOnceUpsertSinkDecision struct {
	Action   ExactlyOnceUpsertSinkAction
	Record   ExactlyOnceUpsertSinkRecord
	Snapshot ExactlyOnceUpsertSinkSnapshot
}

// ExactlyOnceUpsertSinkSnapshot is the durable state needed to restart a
// sink without reapplying a committed source sequence. The embedding service
// must persist this state atomically with its external upsert for exactly-once
// effects across process or storage failures.
type ExactlyOnceUpsertSinkSnapshot struct {
	Source               string
	HasCommittedSequence bool
	CommittedSequence    uint64
	LastOutputID         string
	Pending              *ExactlyOnceUpsertSinkRecord
}

// ExactlyOnceUpsertSink coordinates ordered, idempotent output application.
// It never invokes external callbacks and performs no filesystem or network
// work.
type ExactlyOnceUpsertSink struct {
	mu                   sync.RWMutex
	source               string
	hasCommittedSequence bool
	committedSequence    uint64
	lastOutputID         string
	pending              *ExactlyOnceUpsertSinkRecord
}

// NewExactlyOnceUpsertSink creates a sink for one source.
func NewExactlyOnceUpsertSink(source string) (*ExactlyOnceUpsertSink, error) {
	if err := validateExactlyOnceUpsertSinkIdentity(source); err != nil {
		return nil, err
	}
	return &ExactlyOnceUpsertSink{source: source}, nil
}

// NewExactlyOnceUpsertSinkFromSnapshot restores a validated snapshot.
func NewExactlyOnceUpsertSinkFromSnapshot(snapshot ExactlyOnceUpsertSinkSnapshot) (*ExactlyOnceUpsertSink, error) {
	if err := validateExactlyOnceUpsertSinkSnapshot(snapshot); err != nil {
		return nil, err
	}
	sink, err := NewExactlyOnceUpsertSink(snapshot.Source)
	if err != nil {
		return nil, err
	}
	if err := sink.Restore(snapshot); err != nil {
		return nil, err
	}
	return sink, nil
}

// NewExactlyOnceUpsertSinkFromBinary restores a compact binary snapshot.
func NewExactlyOnceUpsertSinkFromBinary(data []byte) (*ExactlyOnceUpsertSink, error) {
	snapshot, err := UnmarshalExactlyOnceUpsertSinkSnapshot(data)
	if err != nil {
		return nil, err
	}
	return NewExactlyOnceUpsertSinkFromSnapshot(snapshot)
}

// Begin records one record as pending, or returns the action for a retry.
func (sink *ExactlyOnceUpsertSink) Begin(record ExactlyOnceUpsertSinkRecord) (ExactlyOnceUpsertSinkDecision, error) {
	if sink == nil {
		return ExactlyOnceUpsertSinkDecision{}, ErrExactlyOnceUpsertSinkInvalid
	}
	if err := validateExactlyOnceUpsertSinkRecord(record); err != nil {
		return ExactlyOnceUpsertSinkDecision{}, err
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.pending != nil {
		if exactlyOnceUpsertSinkRecordsEqual(*sink.pending, record) {
			return ExactlyOnceUpsertSinkDecision{
				Action:   ExactlyOnceUpsertSinkResume,
				Record:   cloneExactlyOnceUpsertSinkRecord(*sink.pending),
				Snapshot: sink.snapshotLocked(),
			}, nil
		}
		return ExactlyOnceUpsertSinkDecision{}, ErrExactlyOnceUpsertSinkPending
	}

	expected := uint64(1)
	if sink.hasCommittedSequence {
		if record.Sequence <= sink.committedSequence {
			if record.Sequence == sink.committedSequence && record.OutputID != sink.lastOutputID {
				return ExactlyOnceUpsertSinkDecision{}, ErrExactlyOnceUpsertSinkConflict
			}
			return ExactlyOnceUpsertSinkDecision{
				Action:   ExactlyOnceUpsertSinkSkip,
				Record:   cloneExactlyOnceUpsertSinkRecord(record),
				Snapshot: sink.snapshotLocked(),
			}, nil
		}
		if sink.committedSequence == ^uint64(0) {
			return ExactlyOnceUpsertSinkDecision{}, ErrExactlyOnceUpsertSinkSequenceExhausted
		}
		expected = sink.committedSequence + 1
	}
	if record.Sequence < expected {
		return ExactlyOnceUpsertSinkDecision{}, ErrExactlyOnceUpsertSinkOverlap
	}
	if record.Sequence > expected {
		return ExactlyOnceUpsertSinkDecision{}, ErrExactlyOnceUpsertSinkGap
	}

	sink.pending = cloneExactlyOnceUpsertSinkRecordPtr(&record)
	return ExactlyOnceUpsertSinkDecision{
		Action:   ExactlyOnceUpsertSinkApply,
		Record:   cloneExactlyOnceUpsertSinkRecord(record),
		Snapshot: sink.snapshotLocked(),
	}, nil
}

// Commit marks the pending output identity as durably applied. The caller must
// persist the returned snapshot atomically with the external upsert.
func (sink *ExactlyOnceUpsertSink) Commit(outputID string) (ExactlyOnceUpsertSinkSnapshot, error) {
	if sink == nil {
		return ExactlyOnceUpsertSinkSnapshot{}, ErrExactlyOnceUpsertSinkInvalid
	}
	if err := validateExactlyOnceUpsertSinkIdentity(outputID); err != nil {
		return ExactlyOnceUpsertSinkSnapshot{}, err
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.pending == nil {
		if sink.hasCommittedSequence && sink.lastOutputID == outputID {
			return sink.snapshotLocked(), nil
		}
		return ExactlyOnceUpsertSinkSnapshot{}, ErrExactlyOnceUpsertSinkNoPending
	}
	if sink.pending.OutputID != outputID {
		return ExactlyOnceUpsertSinkSnapshot{}, ErrExactlyOnceUpsertSinkConflict
	}
	sink.hasCommittedSequence = true
	sink.committedSequence = sink.pending.Sequence
	sink.lastOutputID = sink.pending.OutputID
	sink.pending = nil
	return sink.snapshotLocked(), nil
}

// Abort removes a pending record so the source can retry it from scratch.
func (sink *ExactlyOnceUpsertSink) Abort(outputID string) (ExactlyOnceUpsertSinkSnapshot, error) {
	if sink == nil {
		return ExactlyOnceUpsertSinkSnapshot{}, ErrExactlyOnceUpsertSinkInvalid
	}
	if err := validateExactlyOnceUpsertSinkIdentity(outputID); err != nil {
		return ExactlyOnceUpsertSinkSnapshot{}, err
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.pending == nil {
		if sink.hasCommittedSequence && sink.lastOutputID == outputID {
			return sink.snapshotLocked(), nil
		}
		return ExactlyOnceUpsertSinkSnapshot{}, ErrExactlyOnceUpsertSinkNoPending
	}
	if sink.pending.OutputID != outputID {
		return ExactlyOnceUpsertSinkSnapshot{}, ErrExactlyOnceUpsertSinkConflict
	}
	sink.pending = nil
	return sink.snapshotLocked(), nil
}

// RestartSequence returns the first source sequence that is not committed.
func (sink *ExactlyOnceUpsertSink) RestartSequence() (uint64, error) {
	if sink == nil {
		return 0, ErrExactlyOnceUpsertSinkInvalid
	}
	sink.mu.RLock()
	defer sink.mu.RUnlock()
	if !sink.hasCommittedSequence {
		return 1, nil
	}
	if sink.committedSequence == ^uint64(0) {
		return 0, ErrExactlyOnceUpsertSinkSequenceExhausted
	}
	return sink.committedSequence + 1, nil
}

// Snapshot returns a copy of the current durable state.
func (sink *ExactlyOnceUpsertSink) Snapshot() ExactlyOnceUpsertSinkSnapshot {
	if sink == nil {
		return ExactlyOnceUpsertSinkSnapshot{}
	}
	sink.mu.RLock()
	defer sink.mu.RUnlock()
	return sink.snapshotLocked()
}

// Restore atomically replaces the sink state with snapshot.
func (sink *ExactlyOnceUpsertSink) Restore(snapshot ExactlyOnceUpsertSinkSnapshot) error {
	if sink == nil {
		return ErrExactlyOnceUpsertSinkInvalid
	}
	if err := validateExactlyOnceUpsertSinkSnapshot(snapshot); err != nil {
		return err
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.source != snapshot.Source {
		return ErrExactlyOnceUpsertSinkSnapshotInvalid
	}
	sink.hasCommittedSequence = snapshot.HasCommittedSequence
	sink.committedSequence = snapshot.CommittedSequence
	sink.lastOutputID = snapshot.LastOutputID
	sink.pending = cloneExactlyOnceUpsertSinkRecordPtr(snapshot.Pending)
	return nil
}

// MarshalBinary encodes the current state in a compact bounded binary format.
func (sink *ExactlyOnceUpsertSink) MarshalBinary() ([]byte, error) {
	if sink == nil {
		return nil, ErrExactlyOnceUpsertSinkInvalid
	}
	return sink.Snapshot().MarshalBinary()
}

// UnmarshalBinary decodes and restores a compact snapshot.
func (sink *ExactlyOnceUpsertSink) UnmarshalBinary(data []byte) error {
	if sink == nil {
		return ErrExactlyOnceUpsertSinkInvalid
	}
	snapshot, err := UnmarshalExactlyOnceUpsertSinkSnapshot(data)
	if err != nil {
		return err
	}
	return sink.Restore(snapshot)
}

// MarshalBinary encodes a snapshot without reflection or JSON field names.
func (snapshot ExactlyOnceUpsertSinkSnapshot) MarshalBinary() ([]byte, error) {
	if err := validateExactlyOnceUpsertSinkSnapshot(snapshot); err != nil {
		return nil, err
	}
	size := exactlyOnceUpsertSinkSnapshotSize(snapshot)
	if size > MaxExactlyOnceUpsertSinkSnapshotBytes {
		return nil, ErrExactlyOnceUpsertSinkSnapshotInvalid
	}
	var buffer bytes.Buffer
	buffer.Grow(size)
	buffer.WriteString("uos1")
	buffer.WriteByte(exactlyOnceUpsertSinkSnapshotVersion)
	var flags byte
	if snapshot.HasCommittedSequence {
		flags |= exactlyOnceUpsertSinkFlagCommitted
	}
	if snapshot.Pending != nil {
		flags |= exactlyOnceUpsertSinkFlagPending
	}
	buffer.WriteByte(flags)
	writeExactlyOnceUpsertSinkString(&buffer, snapshot.Source)
	writeExactlyOnceUpsertSinkUint64(&buffer, snapshot.CommittedSequence)
	writeExactlyOnceUpsertSinkString(&buffer, snapshot.LastOutputID)
	if snapshot.Pending != nil {
		writeExactlyOnceUpsertSinkUint64(&buffer, snapshot.Pending.Sequence)
		writeExactlyOnceUpsertSinkString(&buffer, snapshot.Pending.OutputID)
		writeExactlyOnceUpsertSinkBytes(&buffer, snapshot.Pending.Key)
		writeExactlyOnceUpsertSinkBytes(&buffer, snapshot.Pending.Value)
		if snapshot.Pending.Delete {
			buffer.WriteByte(1)
		} else {
			buffer.WriteByte(0)
		}
	}
	return buffer.Bytes(), nil
}

// UnmarshalBinary decodes a snapshot into the receiver without changing it
// when validation fails.
func (snapshot *ExactlyOnceUpsertSinkSnapshot) UnmarshalBinary(data []byte) error {
	if snapshot == nil {
		return ErrExactlyOnceUpsertSinkSnapshotInvalid
	}
	decoded, err := UnmarshalExactlyOnceUpsertSinkSnapshot(data)
	if err != nil {
		return err
	}
	*snapshot = decoded
	return nil
}

// UnmarshalExactlyOnceUpsertSinkSnapshot decodes and validates a binary state.
func UnmarshalExactlyOnceUpsertSinkSnapshot(data []byte) (ExactlyOnceUpsertSinkSnapshot, error) {
	if len(data) < 6 || len(data) > MaxExactlyOnceUpsertSinkSnapshotBytes || string(data[:4]) != "uos1" || data[4] != exactlyOnceUpsertSinkSnapshotVersion {
		return ExactlyOnceUpsertSinkSnapshot{}, ErrExactlyOnceUpsertSinkSnapshotInvalid
	}
	flags := data[5]
	if flags&^(exactlyOnceUpsertSinkFlagCommitted|exactlyOnceUpsertSinkFlagPending) != 0 {
		return ExactlyOnceUpsertSinkSnapshot{}, ErrExactlyOnceUpsertSinkSnapshotInvalid
	}
	reader := exactlyOnceUpsertSinkReader{data: data, offset: 6}
	snapshot := ExactlyOnceUpsertSinkSnapshot{}
	var ok bool
	if snapshot.Source, ok = reader.string(); !ok {
		return ExactlyOnceUpsertSinkSnapshot{}, ErrExactlyOnceUpsertSinkSnapshotInvalid
	}
	if snapshot.CommittedSequence, ok = reader.uint64(); !ok {
		return ExactlyOnceUpsertSinkSnapshot{}, ErrExactlyOnceUpsertSinkSnapshotInvalid
	}
	if snapshot.LastOutputID, ok = reader.string(); !ok {
		return ExactlyOnceUpsertSinkSnapshot{}, ErrExactlyOnceUpsertSinkSnapshotInvalid
	}
	snapshot.HasCommittedSequence = flags&exactlyOnceUpsertSinkFlagCommitted != 0
	if flags&exactlyOnceUpsertSinkFlagPending != 0 {
		record := ExactlyOnceUpsertSinkRecord{}
		if record.Sequence, ok = reader.uint64(); !ok {
			return ExactlyOnceUpsertSinkSnapshot{}, ErrExactlyOnceUpsertSinkSnapshotInvalid
		}
		if record.OutputID, ok = reader.string(); !ok {
			return ExactlyOnceUpsertSinkSnapshot{}, ErrExactlyOnceUpsertSinkSnapshotInvalid
		}
		if record.Key, ok = reader.bytes(MaxExactlyOnceUpsertSinkKeyBytes); !ok {
			return ExactlyOnceUpsertSinkSnapshot{}, ErrExactlyOnceUpsertSinkSnapshotInvalid
		}
		if record.Value, ok = reader.bytes(MaxExactlyOnceUpsertSinkValueBytes); !ok {
			return ExactlyOnceUpsertSinkSnapshot{}, ErrExactlyOnceUpsertSinkSnapshotInvalid
		}
		if reader.offset >= len(reader.data) {
			return ExactlyOnceUpsertSinkSnapshot{}, ErrExactlyOnceUpsertSinkSnapshotInvalid
		}
		deleteFlag := reader.data[reader.offset]
		reader.offset++
		if deleteFlag > 1 {
			return ExactlyOnceUpsertSinkSnapshot{}, ErrExactlyOnceUpsertSinkSnapshotInvalid
		}
		record.Delete = deleteFlag == 1
		snapshot.Pending = &record
	}
	if reader.offset != len(reader.data) || validateExactlyOnceUpsertSinkSnapshot(snapshot) != nil {
		return ExactlyOnceUpsertSinkSnapshot{}, ErrExactlyOnceUpsertSinkSnapshotInvalid
	}
	return snapshot, nil
}

func (sink *ExactlyOnceUpsertSink) snapshotLocked() ExactlyOnceUpsertSinkSnapshot {
	return ExactlyOnceUpsertSinkSnapshot{
		Source:               sink.source,
		HasCommittedSequence: sink.hasCommittedSequence,
		CommittedSequence:    sink.committedSequence,
		LastOutputID:         sink.lastOutputID,
		Pending:              cloneExactlyOnceUpsertSinkRecordPtr(sink.pending),
	}
}

func cloneExactlyOnceUpsertSinkRecordPtr(record *ExactlyOnceUpsertSinkRecord) *ExactlyOnceUpsertSinkRecord {
	if record == nil {
		return nil
	}
	clone := cloneExactlyOnceUpsertSinkRecord(*record)
	return &clone
}

func cloneExactlyOnceUpsertSinkRecord(record ExactlyOnceUpsertSinkRecord) ExactlyOnceUpsertSinkRecord {
	record.Key = append([]byte(nil), record.Key...)
	record.Value = append([]byte(nil), record.Value...)
	return record
}

func exactlyOnceUpsertSinkRecordsEqual(left, right ExactlyOnceUpsertSinkRecord) bool {
	return left.Sequence == right.Sequence && left.OutputID == right.OutputID && left.Delete == right.Delete && bytes.Equal(left.Key, right.Key) && bytes.Equal(left.Value, right.Value)
}

func validateExactlyOnceUpsertSinkSnapshot(snapshot ExactlyOnceUpsertSinkSnapshot) error {
	if err := validateExactlyOnceUpsertSinkIdentity(snapshot.Source); err != nil {
		return ErrExactlyOnceUpsertSinkSnapshotInvalid
	}
	if !snapshot.HasCommittedSequence {
		if snapshot.CommittedSequence != 0 || snapshot.LastOutputID != "" {
			return ErrExactlyOnceUpsertSinkSnapshotInvalid
		}
	} else if err := validateExactlyOnceUpsertSinkIdentity(snapshot.LastOutputID); err != nil {
		return ErrExactlyOnceUpsertSinkSnapshotInvalid
	}
	if snapshot.Pending == nil {
		return nil
	}
	if err := validateExactlyOnceUpsertSinkRecord(*snapshot.Pending); err != nil {
		return ErrExactlyOnceUpsertSinkSnapshotInvalid
	}
	expected := uint64(1)
	if snapshot.HasCommittedSequence {
		if snapshot.CommittedSequence == ^uint64(0) {
			return ErrExactlyOnceUpsertSinkSnapshotInvalid
		}
		expected = snapshot.CommittedSequence + 1
	}
	if snapshot.Pending.Sequence != expected {
		return ErrExactlyOnceUpsertSinkSnapshotInvalid
	}
	return nil
}

func validateExactlyOnceUpsertSinkRecord(record ExactlyOnceUpsertSinkRecord) error {
	if record.Sequence == 0 || validateExactlyOnceUpsertSinkIdentity(record.OutputID) != nil || len(record.Key) == 0 || len(record.Key) > MaxExactlyOnceUpsertSinkKeyBytes || len(record.Value) > MaxExactlyOnceUpsertSinkValueBytes {
		return ErrExactlyOnceUpsertSinkInvalid
	}
	return nil
}

func validateExactlyOnceUpsertSinkIdentity(value string) error {
	if value == "" || len(value) > MaxExactlyOnceUpsertSinkIdentityBytes || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
		return ErrExactlyOnceUpsertSinkInvalid
	}
	return nil
}

func exactlyOnceUpsertSinkSnapshotSize(snapshot ExactlyOnceUpsertSinkSnapshot) int {
	size := 6 + 2 + len(snapshot.Source) + 8 + 2 + len(snapshot.LastOutputID)
	if snapshot.Pending != nil {
		size += 8 + 2 + len(snapshot.Pending.OutputID) + 4 + len(snapshot.Pending.Key) + 4 + len(snapshot.Pending.Value) + 1
	}
	return size
}

func writeExactlyOnceUpsertSinkString(buffer *bytes.Buffer, value string) {
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], uint16(len(value)))
	buffer.Write(encoded[:])
	buffer.WriteString(value)
}

func writeExactlyOnceUpsertSinkBytes(buffer *bytes.Buffer, value []byte) {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], uint32(len(value)))
	buffer.Write(encoded[:])
	buffer.Write(value)
}

func writeExactlyOnceUpsertSinkUint64(buffer *bytes.Buffer, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	buffer.Write(encoded[:])
}

type exactlyOnceUpsertSinkReader struct {
	data   []byte
	offset int
}

func (reader *exactlyOnceUpsertSinkReader) string() (string, bool) {
	if reader.offset+2 > len(reader.data) {
		return "", false
	}
	length := int(binary.BigEndian.Uint16(reader.data[reader.offset : reader.offset+2]))
	reader.offset += 2
	if length > MaxExactlyOnceUpsertSinkIdentityBytes || reader.offset+length > len(reader.data) {
		return "", false
	}
	value := string(reader.data[reader.offset : reader.offset+length])
	reader.offset += length
	return value, true
}

func (reader *exactlyOnceUpsertSinkReader) bytes(max int) ([]byte, bool) {
	if reader.offset+4 > len(reader.data) {
		return nil, false
	}
	length := uint64(binary.BigEndian.Uint32(reader.data[reader.offset : reader.offset+4]))
	reader.offset += 4
	if length > uint64(max) || length > uint64(len(reader.data)-reader.offset) {
		return nil, false
	}
	value := append([]byte(nil), reader.data[reader.offset:reader.offset+int(length)]...)
	reader.offset += int(length)
	return value, true
}

func (reader *exactlyOnceUpsertSinkReader) uint64() (uint64, bool) {
	if reader.offset+8 > len(reader.data) {
		return 0, false
	}
	value := binary.BigEndian.Uint64(reader.data[reader.offset : reader.offset+8])
	reader.offset += 8
	return value, true
}
