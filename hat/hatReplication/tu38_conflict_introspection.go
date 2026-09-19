package hatReplication

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"strings"
	"sync"
	"time"
)

var (
	ErrConflictIntrospectionLogNil     = errors.New("hatriecache: conflict introspection log is nil")
	ErrConflictIntrospectionInvalid    = errors.New("hatriecache: conflict introspection event is invalid")
	ErrConflictIntrospectionOptions    = errors.New("hatriecache: conflict introspection options are invalid")
	ErrConflictIntrospectionChecksum   = errors.New("hatriecache: conflict introspection checksum mismatch")
	ErrConflictIntrospectionHistoryGap = errors.New("hatriecache: conflict introspection history gap")
	ErrConflictIntrospectionLimit      = errors.New("hatriecache: conflict introspection limit exceeded")
)

const (
	DefaultConflictIntrospectionEvents  = 1024
	MaxConflictIntrospectionEvents      = 65536
	MaxConflictIntrospectionStringBytes = 128
	MaxConflictIntrospectionBytes       = 8 << 20
	MaxConflictIntrospectionReplay      = 65536

	conflictIntrospectionHeaderBytes = 4 + 1 + 4 + 8
	conflictIntrospectionCRCBytes    = 4
)

var conflictIntrospectionCRC32C = crc32.MakeTable(crc32.Castagnoli)

const conflictIntrospectionMagic = "CIF1"

// ConflictIntrospectionDecision describes the result recorded for one
// conflict. The event contains versions and a digest, never a raw key or
// value.
type ConflictIntrospectionDecision uint8

const (
	ConflictIntrospectionLeftWins ConflictIntrospectionDecision = iota + 1
	ConflictIntrospectionRightWins
	ConflictIntrospectionRejected
	ConflictIntrospectionEqual
)

// ConflictIntrospectionEvent is redacted conflict metadata. KeyDigest should
// be a caller-supplied cryptographic digest, preferably an HMAC when keys are
// sensitive. Space and node IDs are bounded metadata strings.
type ConflictIntrospectionEvent struct {
	Sequence          uint64
	Space             string
	KeyDigest         [32]byte
	Left              ConflictVersion
	Right             ConflictVersion
	Decision          ConflictIntrospectionDecision
	TimestampUnixNano int64
}

// ConflictIntrospectionLogOptions bounds retained conflict metadata. The log
// is inactive until a caller creates it and records an event.
type ConflictIntrospectionLogOptions struct {
	MaxEvents int
}

// ConflictIntrospectionLog is a concurrency-safe bounded conflict event log.
// It does not retain raw keys or values and does not install global state.
type ConflictIntrospectionLog struct {
	mu           sync.RWMutex
	maxEvents    int
	events       []ConflictIntrospectionEvent
	head         int
	count        int
	nextSequence uint64
}

// NewConflictIntrospectionLog creates an empty bounded event log.
func NewConflictIntrospectionLog(options ConflictIntrospectionLogOptions) (*ConflictIntrospectionLog, error) {
	maxEvents := options.MaxEvents
	if maxEvents == 0 {
		maxEvents = DefaultConflictIntrospectionEvents
	}
	if maxEvents < 1 || maxEvents > MaxConflictIntrospectionEvents {
		return nil, fmt.Errorf("%w: max events must be between 1 and %d", ErrConflictIntrospectionOptions, MaxConflictIntrospectionEvents)
	}
	return &ConflictIntrospectionLog{
		maxEvents: maxEvents,
		events:    make([]ConflictIntrospectionEvent, maxEvents),
	}, nil
}

// Record validates and appends one event, assigning its monotone sequence.
// When the ring is full, the oldest event is evicted.
func (log *ConflictIntrospectionLog) Record(event ConflictIntrospectionEvent) (ConflictIntrospectionEvent, error) {
	if log == nil {
		return ConflictIntrospectionEvent{}, ErrConflictIntrospectionLogNil
	}
	if event.Sequence != 0 {
		return ConflictIntrospectionEvent{}, fmt.Errorf("%w: sequence is assigned by the log", ErrConflictIntrospectionInvalid)
	}
	if err := validateConflictIntrospectionEvent(event); err != nil {
		return ConflictIntrospectionEvent{}, err
	}
	if event.TimestampUnixNano == 0 {
		event.TimestampUnixNano = time.Now().UnixNano()
	}

	log.mu.Lock()
	defer log.mu.Unlock()
	if log.nextSequence == ^uint64(0) {
		return ConflictIntrospectionEvent{}, fmt.Errorf("%w: sequence exhausted", ErrConflictIntrospectionLimit)
	}
	event.Sequence = log.nextSequence + 1
	log.nextSequence = event.Sequence
	if log.count < log.maxEvents {
		log.events[(log.head+log.count)%log.maxEvents] = event
		log.count++
	} else {
		log.events[log.head] = event
		log.head = (log.head + 1) % log.maxEvents
	}
	return event, nil
}

// Snapshot returns all retained events in sequence order.
func (log *ConflictIntrospectionLog) Snapshot() []ConflictIntrospectionEvent {
	if log == nil {
		return nil
	}
	log.mu.RLock()
	defer log.mu.RUnlock()
	return log.snapshotLocked()
}

// Replay returns retained events strictly after afterSequence. A history gap
// is reported instead of silently returning an incomplete stream.
func (log *ConflictIntrospectionLog) Replay(afterSequence uint64, limit int) ([]ConflictIntrospectionEvent, error) {
	if log == nil {
		return nil, ErrConflictIntrospectionLogNil
	}
	if limit == 0 {
		limit = MaxConflictIntrospectionReplay
	}
	if limit < 1 || limit > MaxConflictIntrospectionReplay {
		return nil, fmt.Errorf("%w: replay limit must be between 1 and %d", ErrConflictIntrospectionLimit, MaxConflictIntrospectionReplay)
	}

	log.mu.RLock()
	defer log.mu.RUnlock()
	if log.count == 0 {
		return nil, nil
	}
	oldest := log.nextSequence - uint64(log.count) + 1
	if afterSequence < oldest-1 {
		return nil, fmt.Errorf("%w: requested after sequence %d, oldest retained sequence %d", ErrConflictIntrospectionHistoryGap, afterSequence, oldest)
	}
	start := afterSequence + 1
	if start < oldest {
		start = oldest
	}
	if start > log.nextSequence {
		return nil, nil
	}
	available := log.nextSequence - start + 1
	if available > uint64(limit) {
		available = uint64(limit)
	}
	result := make([]ConflictIntrospectionEvent, int(available))
	for index := range result {
		sequence := start + uint64(index)
		position := (log.head + int(sequence-oldest)) % log.maxEvents
		result[index] = log.events[position]
	}
	return result, nil
}

// LatestSequence returns the highest assigned sequence, or zero for an empty
// log.
func (log *ConflictIntrospectionLog) LatestSequence() uint64 {
	if log == nil {
		return 0
	}
	log.mu.RLock()
	defer log.mu.RUnlock()
	return log.nextSequence
}

// MarshalBinary encodes the retained redacted events with a bounded CRC32C
// protected format suitable for caller-managed durable storage.
func (log *ConflictIntrospectionLog) MarshalBinary() ([]byte, error) {
	if log == nil {
		return nil, ErrConflictIntrospectionLogNil
	}
	log.mu.RLock()
	defer log.mu.RUnlock()
	return marshalConflictIntrospectionLocked(log)
}

// UnmarshalBinary validates and atomically replaces the retained events. A
// failed decode leaves the existing log unchanged.
func (log *ConflictIntrospectionLog) UnmarshalBinary(data []byte) error {
	if log == nil {
		return ErrConflictIntrospectionLogNil
	}
	parsed, nextSequence, err := decodeConflictIntrospection(data, log.maxEvents)
	if err != nil {
		return err
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	clear(log.events)
	for index, event := range parsed {
		log.events[index] = event
	}
	log.head = 0
	log.count = len(parsed)
	log.nextSequence = nextSequence
	return nil
}

// DecodeConflictIntrospectionLog decodes a bounded event log using the
// caller's retained-event limit.
func DecodeConflictIntrospectionLog(data []byte, options ConflictIntrospectionLogOptions) (*ConflictIntrospectionLog, error) {
	log, err := NewConflictIntrospectionLog(options)
	if err != nil {
		return nil, err
	}
	if err := log.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	return log, nil
}

func validateConflictIntrospectionEvent(event ConflictIntrospectionEvent) error {
	if err := validateConflictIntrospectionString(event.Space, "space", true); err != nil {
		return err
	}
	if _, err := CompareConflictVersions(event.Left, event.Right); err != nil {
		return fmt.Errorf("%w: versions: %v", ErrConflictIntrospectionInvalid, err)
	}
	switch event.Decision {
	case ConflictIntrospectionLeftWins, ConflictIntrospectionRightWins, ConflictIntrospectionRejected, ConflictIntrospectionEqual:
		return nil
	default:
		return fmt.Errorf("%w: unsupported decision %d", ErrConflictIntrospectionInvalid, event.Decision)
	}
}

func validateConflictIntrospectionString(value, field string, required bool) error {
	if required && strings.TrimSpace(value) == "" {
		return fmt.Errorf("%w: %s is required", ErrConflictIntrospectionInvalid, field)
	}
	if len(value) > MaxConflictIntrospectionStringBytes || strings.IndexByte(value, 0) >= 0 {
		return fmt.Errorf("%w: %s is invalid or too long", ErrConflictIntrospectionInvalid, field)
	}
	return nil
}

func (log *ConflictIntrospectionLog) snapshotLocked() []ConflictIntrospectionEvent {
	result := make([]ConflictIntrospectionEvent, log.count)
	for index := range result {
		result[index] = log.events[(log.head+index)%log.maxEvents]
	}
	return result
}

func marshalConflictIntrospectionLocked(log *ConflictIntrospectionLog) ([]byte, error) {
	events := log.snapshotLocked()
	size := conflictIntrospectionHeaderBytes + conflictIntrospectionCRCBytes
	for _, event := range events {
		size += conflictIntrospectionEventSize(event)
	}
	if size > MaxConflictIntrospectionBytes {
		return nil, fmt.Errorf("%w: encoded log exceeds %d bytes", ErrConflictIntrospectionLimit, MaxConflictIntrospectionBytes)
	}
	data := make([]byte, size)
	copy(data[:4], conflictIntrospectionMagic)
	data[4] = 1
	binary.LittleEndian.PutUint32(data[5:9], uint32(len(events)))
	binary.LittleEndian.PutUint64(data[9:17], log.nextSequence)
	offset := conflictIntrospectionHeaderBytes
	for _, event := range events {
		offset = appendConflictIntrospectionEvent(data, offset, event)
	}
	binary.LittleEndian.PutUint32(data[offset:], crc32.Checksum(data[:offset], conflictIntrospectionCRC32C))
	return data, nil
}

func conflictIntrospectionEventSize(event ConflictIntrospectionEvent) int {
	return 8 + 8 + 1 + 2 + len(event.Space) + len(event.KeyDigest) +
		8 + 8 + 2 + len(event.Left.NodeID) +
		8 + 8 + 2 + len(event.Right.NodeID)
}

func appendConflictIntrospectionEvent(data []byte, offset int, event ConflictIntrospectionEvent) int {
	binary.LittleEndian.PutUint64(data[offset:offset+8], event.Sequence)
	offset += 8
	binary.LittleEndian.PutUint64(data[offset:offset+8], uint64(event.TimestampUnixNano))
	offset += 8
	data[offset] = byte(event.Decision)
	offset++
	offset = appendConflictIntrospectionString(data, offset, event.Space)
	copy(data[offset:offset+len(event.KeyDigest)], event.KeyDigest[:])
	offset += len(event.KeyDigest)
	offset = appendConflictVersion(data, offset, event.Left)
	offset = appendConflictVersion(data, offset, event.Right)
	return offset
}

func appendConflictVersion(data []byte, offset int, version ConflictVersion) int {
	binary.LittleEndian.PutUint64(data[offset:offset+8], uint64(version.Timestamp))
	offset += 8
	binary.LittleEndian.PutUint64(data[offset:offset+8], version.Sequence)
	offset += 8
	return appendConflictIntrospectionString(data, offset, version.NodeID)
}

func appendConflictIntrospectionString(data []byte, offset int, value string) int {
	binary.LittleEndian.PutUint16(data[offset:offset+2], uint16(len(value)))
	offset += 2
	copy(data[offset:offset+len(value)], value)
	return offset + len(value)
}

func decodeConflictIntrospection(data []byte, maxEvents int) ([]ConflictIntrospectionEvent, uint64, error) {
	if len(data) > MaxConflictIntrospectionBytes || len(data) < conflictIntrospectionHeaderBytes+conflictIntrospectionCRCBytes {
		return nil, 0, fmt.Errorf("%w: encoded size is invalid", ErrConflictIntrospectionInvalid)
	}
	checksumOffset := len(data) - conflictIntrospectionCRCBytes
	wantChecksum := binary.LittleEndian.Uint32(data[checksumOffset:])
	if got := crc32.Checksum(data[:checksumOffset], conflictIntrospectionCRC32C); got != wantChecksum {
		return nil, 0, ErrConflictIntrospectionChecksum
	}
	if string(data[:4]) != conflictIntrospectionMagic || data[4] != 1 {
		return nil, 0, fmt.Errorf("%w: unsupported format", ErrConflictIntrospectionInvalid)
	}
	count := binary.LittleEndian.Uint32(data[5:9])
	nextSequence := binary.LittleEndian.Uint64(data[9:17])
	if count > uint32(maxEvents) || (count == 0 && nextSequence != 0) || (count > 0 && nextSequence < uint64(count)) {
		return nil, 0, fmt.Errorf("%w: event count and sequence are inconsistent", ErrConflictIntrospectionInvalid)
	}
	events := make([]ConflictIntrospectionEvent, int(count))
	offset := conflictIntrospectionHeaderBytes
	firstSequence := uint64(0)
	if count > 0 {
		firstSequence = nextSequence - uint64(count) + 1
	}
	for index := range events {
		var err error
		events[index], offset, err = decodeConflictIntrospectionEvent(data, offset, checksumOffset)
		if err != nil {
			return nil, 0, err
		}
		if events[index].Sequence != firstSequence+uint64(index) {
			return nil, 0, fmt.Errorf("%w: event sequence is not contiguous", ErrConflictIntrospectionInvalid)
		}
		if err := validateConflictIntrospectionEvent(events[index]); err != nil {
			return nil, 0, err
		}
	}
	if offset != checksumOffset {
		return nil, 0, fmt.Errorf("%w: trailing bytes", ErrConflictIntrospectionInvalid)
	}
	return events, nextSequence, nil
}

func decodeConflictIntrospectionEvent(data []byte, offset, limit int) (ConflictIntrospectionEvent, int, error) {
	var event ConflictIntrospectionEvent
	if offset+8+8+1 > limit {
		return event, offset, fmt.Errorf("%w: truncated event", ErrConflictIntrospectionInvalid)
	}
	event.Sequence = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8
	event.TimestampUnixNano = int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
	offset += 8
	event.Decision = ConflictIntrospectionDecision(data[offset])
	offset++
	var err error
	event.Space, offset, err = decodeConflictIntrospectionString(data, offset, limit, true)
	if err != nil {
		return event, offset, err
	}
	if offset+len(event.KeyDigest) > limit {
		return event, offset, fmt.Errorf("%w: truncated key digest", ErrConflictIntrospectionInvalid)
	}
	copy(event.KeyDigest[:], data[offset:offset+len(event.KeyDigest)])
	offset += len(event.KeyDigest)
	event.Left, offset, err = decodeConflictVersion(data, offset, limit)
	if err != nil {
		return event, offset, err
	}
	event.Right, offset, err = decodeConflictVersion(data, offset, limit)
	return event, offset, err
}

func decodeConflictVersion(data []byte, offset, limit int) (ConflictVersion, int, error) {
	var version ConflictVersion
	if offset+8+8 > limit {
		return version, offset, fmt.Errorf("%w: truncated conflict version", ErrConflictIntrospectionInvalid)
	}
	version.Timestamp = int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
	offset += 8
	version.Sequence = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8
	nodeID, offset, err := decodeConflictIntrospectionString(data, offset, limit, true)
	version.NodeID = nodeID
	return version, offset, err
}

func decodeConflictIntrospectionString(data []byte, offset, limit int, required bool) (string, int, error) {
	if offset+2 > limit {
		return "", offset, fmt.Errorf("%w: truncated string length", ErrConflictIntrospectionInvalid)
	}
	length := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
	offset += 2
	if length > MaxConflictIntrospectionStringBytes || offset+length > limit {
		return "", offset, fmt.Errorf("%w: string length is invalid", ErrConflictIntrospectionInvalid)
	}
	value := string(data[offset : offset+length])
	offset += length
	if err := validateConflictIntrospectionString(value, "string", required); err != nil {
		return "", offset, err
	}
	return value, offset, nil
}
