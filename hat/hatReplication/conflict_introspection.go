package hatReplication

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
)

const (
	// ConflictEventDigestSize is the fixed size of the caller-provided key digest.
	ConflictEventDigestSize = 16

	// DefaultConflictEventCapacity bounds the retained event history when no
	// explicit capacity is supplied.
	DefaultConflictEventCapacity = 1024
	// MaxConflictEventCapacity prevents an accidental unbounded event journal.
	MaxConflictEventCapacity = 1 << 16
	// DefaultConflictEventMaxFieldBytes bounds names and decision labels.
	DefaultConflictEventMaxFieldBytes = 256
	// MaxConflictEventMaxFieldBytes bounds one event field even for trusted callers.
	MaxConflictEventMaxFieldBytes = 4096
	// MaxConflictEventSnapshotBytes bounds a restore input before allocation.
	MaxConflictEventSnapshotBytes = 16 << 20
)

var (
	ErrConflictEventLogNil            = errors.New("hatriecache: conflict event log is nil")
	ErrConflictEventCapacityInvalid   = errors.New("hatriecache: conflict event capacity is invalid")
	ErrConflictEventFieldLimitInvalid = errors.New("hatriecache: conflict event field limit is invalid")
	ErrConflictEventSpaceRequired     = errors.New("hatriecache: conflict event space is required")
	ErrConflictEventKeyDigestRequired = errors.New("hatriecache: conflict event key digest is required")
	ErrConflictEventSourceRequired    = errors.New("hatriecache: conflict event source is required")
	ErrConflictEventDecisionInvalid   = errors.New("hatriecache: conflict event decision is invalid")
	ErrConflictEventFieldTooLarge     = errors.New("hatriecache: conflict event field is too large")
	ErrConflictEventHistoryGap        = errors.New("hatriecache: conflict event history gap")
	ErrConflictEventLimitInvalid      = errors.New("hatriecache: conflict event read limit is invalid")
	ErrConflictEventContextNil        = errors.New("hatriecache: conflict event context is nil")
	ErrConflictEventSequenceExhausted = errors.New("hatriecache: conflict event sequence exhausted")
	ErrConflictEventSnapshotInvalid   = errors.New("hatriecache: conflict event snapshot is invalid")
	ErrConflictEventSnapshotTooLarge  = errors.New("hatriecache: conflict event snapshot is too large")
)

// ConflictDecision describes the policy decision recorded for a conflict.
type ConflictDecision string

const (
	ConflictDecisionLastWriteWins  ConflictDecision = "last-write-wins"
	ConflictDecisionSourcePriority ConflictDecision = "source-priority"
	ConflictDecisionReject         ConflictDecision = "reject"
)

// ConflictEvent is a redacted conflict record. KeyDigest deliberately carries
// no raw key bytes; callers choose the digest algorithm and keep the key out of
// this journal.
type ConflictEvent struct {
	Sequence     uint64
	Timestamp    int64
	Space        string
	KeyDigest    [ConflictEventDigestSize]byte
	WinnerSource string
	LoserSource  string
	Decision     ConflictDecision
}

// ConflictEventLogOptions configures one bounded event journal.
type ConflictEventLogOptions struct {
	Capacity      int
	MaxFieldBytes int
}

// ConflictEventLogStats describes the current retention window.
type ConflictEventLogStats struct {
	Capacity      int
	Retained      int
	FirstSequence uint64
	NextSequence  uint64
	Dropped       uint64
}

// ConflictEventLog is an opt-in bounded redacted event journal. It is safe for
// concurrent append, replay, waiting, and snapshot/restore operations.
type ConflictEventLog struct {
	mu            sync.Mutex
	capacity      int
	maxFieldBytes int
	events        []ConflictEvent
	waiters       []chan struct{}
	start         int
	count         int
	nextSequence  uint64
	dropped       uint64
}

// NewConflictEventLog creates a bounded conflict journal. Zero options select
// bounded defaults; the journal does not hook into conflict resolution unless a
// caller explicitly appends events.
func NewConflictEventLog(options ConflictEventLogOptions) (*ConflictEventLog, error) {
	capacity := options.Capacity
	if capacity == 0 {
		capacity = DefaultConflictEventCapacity
	}
	if capacity < 1 || capacity > MaxConflictEventCapacity {
		return nil, fmt.Errorf("%w: got %d, want 1..%d", ErrConflictEventCapacityInvalid, capacity, MaxConflictEventCapacity)
	}
	maxFieldBytes := options.MaxFieldBytes
	if maxFieldBytes == 0 {
		maxFieldBytes = DefaultConflictEventMaxFieldBytes
	}
	if maxFieldBytes < 1 || maxFieldBytes > MaxConflictEventMaxFieldBytes {
		return nil, fmt.Errorf("%w: got %d, want 1..%d", ErrConflictEventFieldLimitInvalid, maxFieldBytes, MaxConflictEventMaxFieldBytes)
	}
	return &ConflictEventLog{
		capacity:      capacity,
		maxFieldBytes: maxFieldBytes,
		events:        make([]ConflictEvent, capacity),
		nextSequence:  1,
	}, nil
}

// Append adds one redacted event and returns its monotonically increasing
// cursor sequence.
func (log *ConflictEventLog) Append(event ConflictEvent) (uint64, error) {
	if log == nil {
		return 0, ErrConflictEventLogNil
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if err := validateConflictEvent(event, log.maxFieldBytes); err != nil {
		return 0, err
	}
	if log.nextSequence == 0 || log.nextSequence == math.MaxUint64 {
		return 0, ErrConflictEventSequenceExhausted
	}
	sequence := log.nextSequence
	event.Sequence = sequence
	index := (log.start + log.count) % log.capacity
	if log.count == log.capacity {
		index = log.start
		log.start = (log.start + 1) % log.capacity
		log.dropped++
	} else {
		log.count++
	}
	log.events[index] = event
	log.nextSequence++
	log.signalWaitersLocked()
	return sequence, nil
}

// ReadAfter returns at most limit events strictly after after. A history-gap
// error means the cursor is older than the retained ring and must be recovered
// from a newer snapshot or a caller-owned durable store.
func (log *ConflictEventLog) ReadAfter(after uint64, limit int) ([]ConflictEvent, error) {
	if log == nil {
		return nil, ErrConflictEventLogNil
	}
	if limit <= 0 {
		return nil, ErrConflictEventLimitInvalid
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	return log.readAfterLocked(after, limit)
}

// WaitAfter waits without creating a per-waiter goroutine until an event after
// after is retained, the cursor falls behind, or ctx is canceled.
func (log *ConflictEventLog) WaitAfter(ctx context.Context, after uint64, limit int) ([]ConflictEvent, error) {
	if log == nil {
		return nil, ErrConflictEventLogNil
	}
	if ctx == nil {
		return nil, ErrConflictEventContextNil
	}
	if limit <= 0 {
		return nil, ErrConflictEventLimitInvalid
	}
	for {
		log.mu.Lock()
		events, err := log.readAfterLocked(after, limit)
		if err != nil || len(events) > 0 {
			log.mu.Unlock()
			return events, err
		}
		waiter := make(chan struct{}, 1)
		log.waiters = append(log.waiters, waiter)
		log.mu.Unlock()
		select {
		case <-ctx.Done():
			log.removeWaiter(waiter)
			return nil, ctx.Err()
		case <-waiter:
			log.removeWaiter(waiter)
		}
	}
}

// Snapshot returns a copy of the retained events in sequence order.
func (log *ConflictEventLog) Snapshot() []ConflictEvent {
	if log == nil {
		return nil
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	return log.snapshotLocked()
}

// Stats returns the current retention window and drop count.
func (log *ConflictEventLog) Stats() ConflictEventLogStats {
	if log == nil {
		return ConflictEventLogStats{}
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	return ConflictEventLogStats{
		Capacity:      log.capacity,
		Retained:      log.count,
		FirstSequence: log.firstSequenceLocked(),
		NextSequence:  log.nextSequence,
		Dropped:       log.dropped,
	}
}

// MarshalBinary returns a deterministic bounded snapshot suitable for a
// caller-owned durable file, object store, or replication record.
func (log *ConflictEventLog) MarshalBinary() ([]byte, error) {
	if log == nil {
		return nil, ErrConflictEventLogNil
	}
	log.mu.Lock()
	defer log.mu.Unlock()

	encoded := make([]byte, 0, conflictEventSnapshotHeaderSize+log.count*64)
	encoded = append(encoded, conflictEventSnapshotMagic[:]...)
	encoded = append(encoded, conflictEventSnapshotVersion)
	encoded = append(encoded, 0)
	encoded = appendConflictEventU32(encoded, uint32(log.capacity))
	encoded = appendConflictEventU32(encoded, uint32(log.maxFieldBytes))
	encoded = appendConflictEventU64(encoded, log.nextSequence)
	encoded = appendConflictEventU64(encoded, log.dropped)
	encoded = appendConflictEventU32(encoded, uint32(log.count))
	for index := 0; index < log.count; index++ {
		event := log.events[(log.start+index)%log.capacity]
		encoded = appendConflictEventU64(encoded, event.Sequence)
		encoded = appendConflictEventU64(encoded, uint64(event.Timestamp))
		encoded = append(encoded, event.KeyDigest[:]...)
		encoded = appendConflictEventString(encoded, event.Space)
		encoded = appendConflictEventString(encoded, event.WinnerSource)
		encoded = appendConflictEventString(encoded, event.LoserSource)
		encoded = appendConflictEventString(encoded, string(event.Decision))
	}
	if len(encoded) > MaxConflictEventSnapshotBytes {
		return nil, ErrConflictEventSnapshotTooLarge
	}
	return encoded, nil
}

// UnmarshalBinary atomically replaces log state from a validated snapshot.
func (log *ConflictEventLog) UnmarshalBinary(encoded []byte) error {
	if log == nil {
		return ErrConflictEventLogNil
	}
	if len(encoded) > MaxConflictEventSnapshotBytes {
		return ErrConflictEventSnapshotTooLarge
	}
	parsed, err := parseConflictEventSnapshot(encoded)
	if err != nil {
		return err
	}

	log.mu.Lock()
	log.capacity = parsed.capacity
	log.maxFieldBytes = parsed.maxFieldBytes
	log.events = parsed.events
	log.start = 0
	log.count = parsed.count
	log.nextSequence = parsed.nextSequence
	log.dropped = parsed.dropped
	log.signalWaitersLocked()
	log.mu.Unlock()
	return nil
}

func (log *ConflictEventLog) signalWaitersLocked() {
	for _, waiter := range log.waiters {
		select {
		case waiter <- struct{}{}:
		default:
		}
	}
}

func (log *ConflictEventLog) removeWaiter(waiter chan struct{}) {
	log.mu.Lock()
	defer log.mu.Unlock()
	for index, candidate := range log.waiters {
		if candidate != waiter {
			continue
		}
		copy(log.waiters[index:], log.waiters[index+1:])
		log.waiters[len(log.waiters)-1] = nil
		log.waiters = log.waiters[:len(log.waiters)-1]
		return
	}
}

func (log *ConflictEventLog) readAfterLocked(after uint64, limit int) ([]ConflictEvent, error) {
	first := log.firstSequenceLocked()
	if after < first-1 {
		return nil, fmt.Errorf("%w: after=%d first=%d", ErrConflictEventHistoryGap, after, first)
	}
	if after >= log.nextSequence-1 || log.count == 0 {
		return nil, nil
	}
	startSequence := after + 1
	available := log.nextSequence - startSequence
	if available > uint64(limit) {
		available = uint64(limit)
	}
	events := make([]ConflictEvent, int(available))
	firstIndex := int(startSequence - first)
	for index := range events {
		events[index] = log.events[(log.start+firstIndex+index)%log.capacity]
	}
	return events, nil
}

func (log *ConflictEventLog) snapshotLocked() []ConflictEvent {
	events := make([]ConflictEvent, log.count)
	for index := range events {
		events[index] = log.events[(log.start+index)%log.capacity]
	}
	return events
}

func (log *ConflictEventLog) firstSequenceLocked() uint64 {
	return log.nextSequence - uint64(log.count)
}

func validateConflictEvent(event ConflictEvent, maxFieldBytes int) error {
	if strings.TrimSpace(event.Space) == "" {
		return ErrConflictEventSpaceRequired
	}
	if event.KeyDigest == [ConflictEventDigestSize]byte{} {
		return ErrConflictEventKeyDigestRequired
	}
	if strings.TrimSpace(event.LoserSource) == "" || (event.Decision != ConflictDecisionReject && strings.TrimSpace(event.WinnerSource) == "") {
		return ErrConflictEventSourceRequired
	}
	switch event.Decision {
	case ConflictDecisionLastWriteWins, ConflictDecisionSourcePriority, ConflictDecisionReject:
	default:
		return ErrConflictEventDecisionInvalid
	}
	if err := validateConflictEventField("space", event.Space, maxFieldBytes); err != nil {
		return err
	}
	if err := validateConflictEventField("winner source", event.WinnerSource, maxFieldBytes); err != nil {
		return err
	}
	if err := validateConflictEventField("loser source", event.LoserSource, maxFieldBytes); err != nil {
		return err
	}
	if err := validateConflictEventField("decision", string(event.Decision), maxFieldBytes); err != nil {
		return err
	}
	return nil
}

func validateConflictEventField(name, value string, maxFieldBytes int) error {
	if len(value) > maxFieldBytes {
		return fmt.Errorf("%w: %s exceeds %d bytes", ErrConflictEventFieldTooLarge, name, maxFieldBytes)
	}
	return nil
}

var conflictEventSnapshotMagic = [4]byte{'H', 'C', 'E', '1'}

const (
	conflictEventSnapshotVersion    = byte(1)
	conflictEventSnapshotHeaderSize = 4 + 1 + 1 + 4 + 4 + 8 + 8 + 4
)

type parsedConflictEventSnapshot struct {
	capacity      int
	maxFieldBytes int
	nextSequence  uint64
	dropped       uint64
	count         int
	events        []ConflictEvent
}

func parseConflictEventSnapshot(encoded []byte) (parsedConflictEventSnapshot, error) {
	var parsed parsedConflictEventSnapshot
	if len(encoded) < conflictEventSnapshotHeaderSize || !bytes.Equal(encoded[:4], conflictEventSnapshotMagic[:]) || encoded[4] != conflictEventSnapshotVersion {
		return parsed, ErrConflictEventSnapshotInvalid
	}
	offset := conflictEventSnapshotHeaderSize
	capacity := int(binary.LittleEndian.Uint32(encoded[6:10]))
	maxFieldBytes := int(binary.LittleEndian.Uint32(encoded[10:14]))
	parsed.nextSequence = binary.LittleEndian.Uint64(encoded[14:22])
	parsed.dropped = binary.LittleEndian.Uint64(encoded[22:30])
	count := int(binary.LittleEndian.Uint32(encoded[30:34]))
	if capacity < 1 || capacity > MaxConflictEventCapacity || maxFieldBytes < 1 || maxFieldBytes > MaxConflictEventMaxFieldBytes || count < 0 || count > capacity || parsed.nextSequence == 0 {
		return parsed, ErrConflictEventSnapshotInvalid
	}
	parsed.capacity = capacity
	parsed.maxFieldBytes = maxFieldBytes
	parsed.count = count
	parsed.events = make([]ConflictEvent, capacity)
	var previousSequence uint64
	for index := 0; index < count; index++ {
		sequence, ok := readConflictEventU64(encoded, &offset)
		if !ok || sequence == 0 || (index > 0 && sequence != previousSequence+1) {
			return parsed, ErrConflictEventSnapshotInvalid
		}
		timestamp, ok := readConflictEventU64(encoded, &offset)
		if !ok || len(encoded)-offset < ConflictEventDigestSize {
			return parsed, ErrConflictEventSnapshotInvalid
		}
		event := ConflictEvent{Sequence: sequence, Timestamp: int64(timestamp)}
		copy(event.KeyDigest[:], encoded[offset:offset+ConflictEventDigestSize])
		offset += ConflictEventDigestSize
		var err error
		if event.Space, err = readConflictEventString(encoded, &offset, maxFieldBytes); err != nil {
			return parsed, err
		}
		if event.WinnerSource, err = readConflictEventString(encoded, &offset, maxFieldBytes); err != nil {
			return parsed, err
		}
		if event.LoserSource, err = readConflictEventString(encoded, &offset, maxFieldBytes); err != nil {
			return parsed, err
		}
		decision, err := readConflictEventString(encoded, &offset, maxFieldBytes)
		if err != nil {
			return parsed, err
		}
		event.Decision = ConflictDecision(decision)
		if err := validateConflictEvent(event, maxFieldBytes); err != nil {
			return parsed, fmt.Errorf("%w: event %d: %v", ErrConflictEventSnapshotInvalid, index, err)
		}
		parsed.events[index] = event
		previousSequence = sequence
	}
	if offset != len(encoded) || (count > 0 && parsed.nextSequence != previousSequence+1) {
		return parsed, ErrConflictEventSnapshotInvalid
	}
	return parsed, nil
}

func appendConflictEventU32(encoded []byte, value uint32) []byte {
	var bytes [4]byte
	binary.LittleEndian.PutUint32(bytes[:], value)
	return append(encoded, bytes[:]...)
}

func appendConflictEventU64(encoded []byte, value uint64) []byte {
	var bytes [8]byte
	binary.LittleEndian.PutUint64(bytes[:], value)
	return append(encoded, bytes[:]...)
}

func appendConflictEventString(encoded []byte, value string) []byte {
	encoded = appendConflictEventU32(encoded, uint32(len(value)))
	return append(encoded, value...)
}

func readConflictEventU64(encoded []byte, offset *int) (uint64, bool) {
	if *offset < 0 || len(encoded)-*offset < 8 {
		return 0, false
	}
	value := binary.LittleEndian.Uint64(encoded[*offset : *offset+8])
	*offset += 8
	return value, true
}

func readConflictEventString(encoded []byte, offset *int, maxFieldBytes int) (string, error) {
	if *offset < 0 || len(encoded)-*offset < 4 {
		return "", ErrConflictEventSnapshotInvalid
	}
	length := int(binary.LittleEndian.Uint32(encoded[*offset : *offset+4]))
	*offset += 4
	if length > maxFieldBytes || length < 0 || len(encoded)-*offset < length {
		return "", ErrConflictEventSnapshotInvalid
	}
	value := string(encoded[*offset : *offset+length])
	*offset += length
	return value, nil
}
