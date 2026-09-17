package hatSql

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultSQLSinkDeliveryAuditCapacity is used when Capacity is zero or
	// negative. The audit is opt-in; constructing one is the opt-in boundary.
	DefaultSQLSinkDeliveryAuditCapacity = 256
	// MaxSQLSinkDeliveryAuditCapacity bounds retained delivery events.
	MaxSQLSinkDeliveryAuditCapacity    = 65536
	maxSQLSinkDeliveryAuditStringBytes = 1024
	maxSQLSinkDeliveryAuditErrorBytes  = 4096
	maxSQLSinkDeliveryAuditProgress    = 1024
	maxSQLSinkDeliveryAuditEvents      = MaxSQLSinkDeliveryAuditCapacity
	maxSQLSinkDeliveryAuditBytes       = 16 << 20
)

var (
	// ErrSQLSinkDeliveryAuditNil reports a method call on a nil audit.
	ErrSQLSinkDeliveryAuditNil = errors.New("SQL sink delivery audit is nil")
	// ErrSQLSinkDeliveryAuditInvalid reports malformed event or snapshot data.
	ErrSQLSinkDeliveryAuditInvalid = errors.New("SQL sink delivery audit is invalid")
)

// SQLSinkDeliveryOutcome identifies the terminal outcome of one sink delivery
// attempt recorded by SQLSinkCommitCoordinator.
type SQLSinkDeliveryOutcome string

const (
	SQLSinkDeliveryCommitted SQLSinkDeliveryOutcome = "committed"
	SQLSinkDeliveryFailed    SQLSinkDeliveryOutcome = "failed"
	SQLSinkDeliveryDuplicate SQLSinkDeliveryOutcome = "duplicate"
	SQLSinkDeliveryConflict  SQLSinkDeliveryOutcome = "conflict"
)

// SQLSinkDeliveryEvent is a bounded, payload-free delivery audit record. The
// Progress entries identify the source frontier acknowledged by the attempt.
// Sequence is assigned by SQLSinkDeliveryAudit.Record.
type SQLSinkDeliveryEvent struct {
	Sequence      uint64                 `json:"sequence"`
	At            time.Time              `json:"at"`
	Sink          string                 `json:"sink"`
	TransactionID string                 `json:"transaction_id"`
	Outcome       SQLSinkDeliveryOutcome `json:"outcome"`
	Progress      []SQLSinkProgress      `json:"progress"`
	Error         string                 `json:"error,omitempty"`
}

// SQLSinkDeliveryAuditOptions bounds retained delivery events.
type SQLSinkDeliveryAuditOptions struct {
	Capacity int `json:"capacity,omitempty"`
}

// SQLSinkDeliveryAuditSnapshot is an independently owned audit checkpoint.
// Events are ordered from oldest retained event to newest retained event.
type SQLSinkDeliveryAuditSnapshot struct {
	Capacity     int                    `json:"capacity"`
	Dropped      uint64                 `json:"dropped"`
	NextSequence uint64                 `json:"next_sequence"`
	Events       []SQLSinkDeliveryEvent `json:"events"`
}

// SQLSinkDeliveryAuditStats is a point-in-time retention summary.
type SQLSinkDeliveryAuditStats struct {
	Capacity     int    `json:"capacity"`
	Retained     int    `json:"retained"`
	Dropped      uint64 `json:"dropped"`
	NextSequence uint64 `json:"next_sequence"`
}

// SQLSinkDeliveryAudit retains a bounded chronological ring of sink outcomes.
// It deliberately stores no sink payloads and can be persisted with
// MarshalBinary.
type SQLSinkDeliveryAudit struct {
	mu       sync.RWMutex
	capacity int
	events   []SQLSinkDeliveryEvent
	head     int
	size     int
	dropped  uint64
	next     uint64
}

// NewSQLSinkDeliveryAudit creates an opt-in bounded delivery audit.
func NewSQLSinkDeliveryAudit(options SQLSinkDeliveryAuditOptions) *SQLSinkDeliveryAudit {
	capacity := normalizeSQLSinkDeliveryAuditCapacity(options.Capacity)
	return &SQLSinkDeliveryAudit{
		capacity: capacity,
		events:   make([]SQLSinkDeliveryEvent, capacity),
		next:     1,
	}
}

// NewSQLSinkDeliveryAuditFromSnapshot restores an audit from a validated
// checkpoint.
func NewSQLSinkDeliveryAuditFromSnapshot(snapshot SQLSinkDeliveryAuditSnapshot) (*SQLSinkDeliveryAudit, error) {
	audit := NewSQLSinkDeliveryAudit(SQLSinkDeliveryAuditOptions{Capacity: snapshot.Capacity})
	if err := audit.Restore(snapshot); err != nil {
		return nil, err
	}
	return audit, nil
}

// Record appends one terminal delivery outcome. The caller must leave
// Sequence at zero; audit sequence numbers are always assigned by the audit.
func (audit *SQLSinkDeliveryAudit) Record(event SQLSinkDeliveryEvent) error {
	if audit == nil {
		return ErrSQLSinkDeliveryAuditNil
	}
	normalized, err := normalizeSQLSinkDeliveryEvent(event, false)
	if err != nil {
		return err
	}
	audit.mu.Lock()
	defer audit.mu.Unlock()
	audit.ensureLocked()
	if audit.next == 0 || audit.next == ^uint64(0) {
		return ErrSQLSinkDeliveryAuditInvalid
	}
	normalized.Sequence = audit.next
	audit.next++
	audit.appendLocked(normalized)
	return nil
}

// Snapshot returns a deterministic, independently owned checkpoint.
func (audit *SQLSinkDeliveryAudit) Snapshot() SQLSinkDeliveryAuditSnapshot {
	if audit == nil {
		return SQLSinkDeliveryAuditSnapshot{}
	}
	audit.mu.RLock()
	defer audit.mu.RUnlock()
	capacity := audit.capacity
	if capacity <= 0 {
		capacity = DefaultSQLSinkDeliveryAuditCapacity
	}
	snapshot := SQLSinkDeliveryAuditSnapshot{
		Capacity:     capacity,
		Dropped:      audit.dropped,
		NextSequence: audit.next,
		Events:       make([]SQLSinkDeliveryEvent, audit.size),
	}
	for index := range snapshot.Events {
		event := audit.events[(audit.head+index)%len(audit.events)]
		snapshot.Events[index] = cloneSQLSinkDeliveryEvent(event)
	}
	return snapshot
}

// Stats returns current ring retention without copying event progress.
func (audit *SQLSinkDeliveryAudit) Stats() SQLSinkDeliveryAuditStats {
	if audit == nil {
		return SQLSinkDeliveryAuditStats{}
	}
	audit.mu.RLock()
	defer audit.mu.RUnlock()
	capacity := audit.capacity
	if capacity <= 0 {
		capacity = DefaultSQLSinkDeliveryAuditCapacity
	}
	return SQLSinkDeliveryAuditStats{
		Capacity:     capacity,
		Retained:     audit.size,
		Dropped:      audit.dropped,
		NextSequence: audit.next,
	}
}

// Restore atomically replaces the retained ring with a validated snapshot.
func (audit *SQLSinkDeliveryAudit) Restore(snapshot SQLSinkDeliveryAuditSnapshot) error {
	if audit == nil {
		return ErrSQLSinkDeliveryAuditNil
	}
	validated, err := normalizeSQLSinkDeliveryAuditSnapshot(snapshot)
	if err != nil {
		return err
	}
	capacity := validated.Capacity
	ring := make([]SQLSinkDeliveryEvent, capacity)
	for index, event := range validated.Events {
		ring[index] = cloneSQLSinkDeliveryEvent(event)
	}
	audit.mu.Lock()
	audit.capacity = capacity
	audit.events = ring
	audit.head = 0
	audit.size = len(validated.Events)
	audit.dropped = validated.Dropped
	audit.next = validated.NextSequence
	audit.mu.Unlock()
	return nil
}

// MarshalBinary encodes the audit checkpoint with bounded length-prefixed
// fields and no reflection or JSON metadata.
func (audit *SQLSinkDeliveryAudit) MarshalBinary() ([]byte, error) {
	if audit == nil {
		return nil, ErrSQLSinkDeliveryAuditNil
	}
	return marshalSQLSinkDeliveryAuditSnapshot(audit.Snapshot())
}

// UnmarshalSQLSinkDeliveryAudit decodes and validates an audit checkpoint.
func UnmarshalSQLSinkDeliveryAudit(data []byte) (SQLSinkDeliveryAuditSnapshot, error) {
	if len(data) > maxSQLSinkDeliveryAuditBytes || len(data) < 5 || !bytes.Equal(data[:4], []byte("HSA1")) || data[4] != 1 {
		return SQLSinkDeliveryAuditSnapshot{}, ErrSQLSinkDeliveryAuditInvalid
	}
	reader := sqlSinkDeliveryAuditReader{data: data, offset: 5}
	capacity, ok := reader.uvarint()
	if !ok || capacity == 0 || capacity > MaxSQLSinkDeliveryAuditCapacity {
		return SQLSinkDeliveryAuditSnapshot{}, ErrSQLSinkDeliveryAuditInvalid
	}
	dropped, ok := reader.uvarint()
	if !ok {
		return SQLSinkDeliveryAuditSnapshot{}, ErrSQLSinkDeliveryAuditInvalid
	}
	next, ok := reader.uvarint()
	if !ok || next == 0 {
		return SQLSinkDeliveryAuditSnapshot{}, ErrSQLSinkDeliveryAuditInvalid
	}
	eventCount, ok := reader.uvarint()
	if !ok || eventCount > uint64(capacity) || eventCount > maxSQLSinkDeliveryAuditEvents {
		return SQLSinkDeliveryAuditSnapshot{}, ErrSQLSinkDeliveryAuditInvalid
	}
	snapshot := SQLSinkDeliveryAuditSnapshot{
		Capacity:     int(capacity),
		Dropped:      dropped,
		NextSequence: next,
		Events:       make([]SQLSinkDeliveryEvent, 0, int(eventCount)),
	}
	for index := uint64(0); index < eventCount; index++ {
		sequence, ok := reader.uvarint()
		if !ok {
			return SQLSinkDeliveryAuditSnapshot{}, ErrSQLSinkDeliveryAuditInvalid
		}
		hasTime, ok := reader.byte()
		if !ok || hasTime > 1 {
			return SQLSinkDeliveryAuditSnapshot{}, ErrSQLSinkDeliveryAuditInvalid
		}
		var at time.Time
		if hasTime == 1 {
			nanos, ok := reader.varint()
			if !ok {
				return SQLSinkDeliveryAuditSnapshot{}, ErrSQLSinkDeliveryAuditInvalid
			}
			at = time.Unix(0, nanos).UTC()
		}
		sink, ok := reader.string(maxSQLSinkDeliveryAuditStringBytes)
		if !ok {
			return SQLSinkDeliveryAuditSnapshot{}, ErrSQLSinkDeliveryAuditInvalid
		}
		transactionID, ok := reader.string(maxSQLSinkDeliveryAuditStringBytes)
		if !ok {
			return SQLSinkDeliveryAuditSnapshot{}, ErrSQLSinkDeliveryAuditInvalid
		}
		outcomeCode, ok := reader.byte()
		outcome, validOutcome := sqlSinkDeliveryOutcomeFromCode(outcomeCode)
		if !ok || !validOutcome {
			return SQLSinkDeliveryAuditSnapshot{}, ErrSQLSinkDeliveryAuditInvalid
		}
		progressCount, ok := reader.uvarint()
		if !ok || progressCount == 0 || progressCount > maxSQLSinkDeliveryAuditProgress {
			return SQLSinkDeliveryAuditSnapshot{}, ErrSQLSinkDeliveryAuditInvalid
		}
		progress := make([]SQLSinkProgress, 0, int(progressCount))
		for progressIndex := uint64(0); progressIndex < progressCount; progressIndex++ {
			progressSink, ok := reader.string(maxSQLSinkDeliveryAuditStringBytes)
			if !ok {
				return SQLSinkDeliveryAuditSnapshot{}, ErrSQLSinkDeliveryAuditInvalid
			}
			partition, ok := reader.string(maxSQLSinkDeliveryAuditStringBytes)
			if !ok {
				return SQLSinkDeliveryAuditSnapshot{}, ErrSQLSinkDeliveryAuditInvalid
			}
			frontier, ok := reader.uvarint()
			if !ok {
				return SQLSinkDeliveryAuditSnapshot{}, ErrSQLSinkDeliveryAuditInvalid
			}
			progress = append(progress, SQLSinkProgress{Sink: progressSink, Partition: partition, Frontier: frontier})
		}
		errorMessage, ok := reader.string(maxSQLSinkDeliveryAuditErrorBytes)
		if !ok {
			return SQLSinkDeliveryAuditSnapshot{}, ErrSQLSinkDeliveryAuditInvalid
		}
		event, err := normalizeSQLSinkDeliveryEvent(SQLSinkDeliveryEvent{
			Sequence:      sequence,
			At:            at,
			Sink:          sink,
			TransactionID: transactionID,
			Outcome:       outcome,
			Progress:      progress,
			Error:         errorMessage,
		}, true)
		if err != nil {
			return SQLSinkDeliveryAuditSnapshot{}, ErrSQLSinkDeliveryAuditInvalid
		}
		snapshot.Events = append(snapshot.Events, event)
	}
	if reader.offset != len(data) {
		return SQLSinkDeliveryAuditSnapshot{}, ErrSQLSinkDeliveryAuditInvalid
	}
	return normalizeSQLSinkDeliveryAuditSnapshot(snapshot)
}

func normalizeSQLSinkDeliveryAuditCapacity(capacity int) int {
	if capacity <= 0 {
		return DefaultSQLSinkDeliveryAuditCapacity
	}
	if capacity > MaxSQLSinkDeliveryAuditCapacity {
		return MaxSQLSinkDeliveryAuditCapacity
	}
	return capacity
}

func (audit *SQLSinkDeliveryAudit) ensureLocked() {
	if audit.capacity <= 0 {
		audit.capacity = DefaultSQLSinkDeliveryAuditCapacity
	}
	if audit.events == nil || len(audit.events) != audit.capacity {
		audit.events = make([]SQLSinkDeliveryEvent, audit.capacity)
		audit.head = 0
		audit.size = 0
	}
	if audit.next == 0 {
		audit.next = 1
	}
}

func (audit *SQLSinkDeliveryAudit) appendLocked(event SQLSinkDeliveryEvent) {
	if audit.size < audit.capacity {
		audit.events[(audit.head+audit.size)%audit.capacity] = event
		audit.size++
		return
	}
	audit.events[audit.head] = event
	audit.head = (audit.head + 1) % audit.capacity
	audit.dropped++
}

func normalizeSQLSinkDeliveryEvent(event SQLSinkDeliveryEvent, requireSequence bool) (SQLSinkDeliveryEvent, error) {
	if requireSequence {
		if event.Sequence == 0 {
			return SQLSinkDeliveryEvent{}, ErrSQLSinkDeliveryAuditInvalid
		}
	} else if event.Sequence != 0 {
		return SQLSinkDeliveryEvent{}, ErrSQLSinkDeliveryAuditInvalid
	}
	event.Sink = strings.TrimSpace(event.Sink)
	event.TransactionID = strings.TrimSpace(event.TransactionID)
	if event.Sink == "" || event.TransactionID == "" || len(event.Sink) > maxSQLSinkDeliveryAuditStringBytes || len(event.TransactionID) > maxSQLSinkDeliveryAuditStringBytes {
		return SQLSinkDeliveryEvent{}, ErrSQLSinkDeliveryAuditInvalid
	}
	if !validSQLSinkDeliveryOutcome(event.Outcome) || len(event.Progress) == 0 || len(event.Progress) > maxSQLSinkDeliveryAuditProgress || len(event.Error) > maxSQLSinkDeliveryAuditErrorBytes {
		return SQLSinkDeliveryEvent{}, ErrSQLSinkDeliveryAuditInvalid
	}
	progress := make([]SQLSinkProgress, len(event.Progress))
	seen := make(map[sqlSinkProgressKey]struct{}, len(event.Progress))
	for index, value := range event.Progress {
		key, normalized, err := normalizeSQLSinkProgress(value)
		if err != nil || normalized.Sink != event.Sink || len(normalized.Sink) > maxSQLSinkDeliveryAuditStringBytes || len(normalized.Partition) > maxSQLSinkDeliveryAuditStringBytes {
			return SQLSinkDeliveryEvent{}, ErrSQLSinkDeliveryAuditInvalid
		}
		if _, found := seen[key]; found {
			return SQLSinkDeliveryEvent{}, ErrSQLSinkDeliveryAuditInvalid
		}
		seen[key] = struct{}{}
		progress[index] = normalized
	}
	sort.Slice(progress, func(left, right int) bool { return progress[left].Partition < progress[right].Partition })
	event.Progress = progress
	if !event.At.IsZero() {
		event.At = event.At.UTC()
	}
	return event, nil
}

func normalizeSQLSinkDeliveryAuditSnapshot(snapshot SQLSinkDeliveryAuditSnapshot) (SQLSinkDeliveryAuditSnapshot, error) {
	if snapshot.Capacity <= 0 {
		snapshot.Capacity = DefaultSQLSinkDeliveryAuditCapacity
	}
	if snapshot.Capacity > MaxSQLSinkDeliveryAuditCapacity || len(snapshot.Events) > snapshot.Capacity || len(snapshot.Events) > maxSQLSinkDeliveryAuditEvents {
		return SQLSinkDeliveryAuditSnapshot{}, ErrSQLSinkDeliveryAuditInvalid
	}
	if snapshot.NextSequence == 0 {
		snapshot.NextSequence = 1
	}
	normalized := make([]SQLSinkDeliveryEvent, len(snapshot.Events))
	var previous uint64
	for index, event := range snapshot.Events {
		value, err := normalizeSQLSinkDeliveryEvent(event, true)
		if err != nil || value.Sequence <= previous {
			return SQLSinkDeliveryAuditSnapshot{}, ErrSQLSinkDeliveryAuditInvalid
		}
		previous = value.Sequence
		normalized[index] = value
	}
	if previous >= snapshot.NextSequence {
		return SQLSinkDeliveryAuditSnapshot{}, ErrSQLSinkDeliveryAuditInvalid
	}
	snapshot.Events = normalized
	return snapshot, nil
}

func cloneSQLSinkDeliveryEvent(event SQLSinkDeliveryEvent) SQLSinkDeliveryEvent {
	event.Progress = cloneSQLSinkProgress(event.Progress)
	return event
}

func validSQLSinkDeliveryOutcome(outcome SQLSinkDeliveryOutcome) bool {
	switch outcome {
	case SQLSinkDeliveryCommitted, SQLSinkDeliveryFailed, SQLSinkDeliveryDuplicate, SQLSinkDeliveryConflict:
		return true
	default:
		return false
	}
}

func sqlSinkDeliveryOutcomeCode(outcome SQLSinkDeliveryOutcome) byte {
	switch outcome {
	case SQLSinkDeliveryCommitted:
		return 1
	case SQLSinkDeliveryFailed:
		return 2
	case SQLSinkDeliveryDuplicate:
		return 3
	case SQLSinkDeliveryConflict:
		return 4
	default:
		return 0
	}
}

func sqlSinkDeliveryOutcomeFromCode(code byte) (SQLSinkDeliveryOutcome, bool) {
	switch code {
	case 1:
		return SQLSinkDeliveryCommitted, true
	case 2:
		return SQLSinkDeliveryFailed, true
	case 3:
		return SQLSinkDeliveryDuplicate, true
	case 4:
		return SQLSinkDeliveryConflict, true
	default:
		return "", false
	}
}

func marshalSQLSinkDeliveryAuditSnapshot(snapshot SQLSinkDeliveryAuditSnapshot) ([]byte, error) {
	validated, err := normalizeSQLSinkDeliveryAuditSnapshot(snapshot)
	if err != nil {
		return nil, err
	}
	encoded := make([]byte, 0, 64+len(validated.Events)*96)
	encoded = append(encoded, 'H', 'S', 'A', '1', 1)
	encoded = appendUvarint(encoded, uint64(validated.Capacity))
	encoded = appendUvarint(encoded, validated.Dropped)
	encoded = appendUvarint(encoded, validated.NextSequence)
	encoded = appendUvarint(encoded, uint64(len(validated.Events)))
	for _, event := range validated.Events {
		encoded = appendUvarint(encoded, event.Sequence)
		if event.At.IsZero() {
			encoded = append(encoded, 0)
		} else {
			encoded = append(encoded, 1)
			encoded = appendVarint(encoded, event.At.UnixNano())
		}
		encoded = appendSQLSinkDeliveryAuditString(encoded, event.Sink)
		encoded = appendSQLSinkDeliveryAuditString(encoded, event.TransactionID)
		encoded = append(encoded, sqlSinkDeliveryOutcomeCode(event.Outcome))
		encoded = appendUvarint(encoded, uint64(len(event.Progress)))
		for _, progress := range event.Progress {
			encoded = appendSQLSinkDeliveryAuditString(encoded, progress.Sink)
			encoded = appendSQLSinkDeliveryAuditString(encoded, progress.Partition)
			encoded = appendUvarint(encoded, progress.Frontier)
		}
		encoded = appendSQLSinkDeliveryAuditString(encoded, event.Error)
	}
	if len(encoded) > maxSQLSinkDeliveryAuditBytes {
		return nil, fmt.Errorf("%w: encoded snapshot exceeds %d bytes", ErrSQLSinkDeliveryAuditInvalid, maxSQLSinkDeliveryAuditBytes)
	}
	return encoded, nil
}

func appendSQLSinkDeliveryAuditString(destination []byte, value string) []byte {
	destination = appendUvarint(destination, uint64(len(value)))
	return append(destination, value...)
}

func appendUvarint(destination []byte, value uint64) []byte {
	var buffer [binary.MaxVarintLen64]byte
	count := binary.PutUvarint(buffer[:], value)
	return append(destination, buffer[:count]...)
}

func appendVarint(destination []byte, value int64) []byte {
	var buffer [binary.MaxVarintLen64]byte
	count := binary.PutVarint(buffer[:], value)
	return append(destination, buffer[:count]...)
}

type sqlSinkDeliveryAuditReader struct {
	data   []byte
	offset int
}

func (reader *sqlSinkDeliveryAuditReader) uvarint() (uint64, bool) {
	if reader.offset >= len(reader.data) {
		return 0, false
	}
	value, count := binary.Uvarint(reader.data[reader.offset:])
	if count <= 0 {
		return 0, false
	}
	reader.offset += count
	return value, true
}

func (reader *sqlSinkDeliveryAuditReader) varint() (int64, bool) {
	if reader.offset >= len(reader.data) {
		return 0, false
	}
	value, count := binary.Varint(reader.data[reader.offset:])
	if count <= 0 {
		return 0, false
	}
	reader.offset += count
	return value, true
}

func (reader *sqlSinkDeliveryAuditReader) byte() (byte, bool) {
	if reader.offset >= len(reader.data) {
		return 0, false
	}
	value := reader.data[reader.offset]
	reader.offset++
	return value, true
}

func (reader *sqlSinkDeliveryAuditReader) string(maxBytes int) (string, bool) {
	length, ok := reader.uvarint()
	if !ok || length > uint64(maxBytes) || length > uint64(len(reader.data)-reader.offset) {
		return "", false
	}
	start := reader.offset
	reader.offset += int(length)
	return string(reader.data[start:reader.offset]), true
}
