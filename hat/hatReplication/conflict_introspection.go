package hatReplication

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"strings"
	"sync"
	"time"
)

var (
	ErrConflictEventLogNil          = errors.New("hatriecache: conflict event log is nil")
	ErrConflictEventCapacityInvalid = errors.New("hatriecache: conflict event log capacity is invalid")
	ErrConflictEventSpaceRequired   = errors.New("hatriecache: conflict event space is required")
	ErrConflictEventVersionInvalid  = errors.New("hatriecache: conflict event version is invalid")
	ErrConflictEventDecisionInvalid = errors.New("hatriecache: conflict event decision is invalid")
	ErrConflictEventIDExhausted     = errors.New("hatriecache: conflict event id is exhausted")
	ErrConflictEventSnapshotInvalid = errors.New("hatriecache: conflict event snapshot is invalid")
	ErrConflictEventSnapshotCorrupt = errors.New("hatriecache: conflict event snapshot is corrupt")
)

const (
	DefaultConflictEventLogCapacity = 1024
	MaxConflictEventLogCapacity     = 65536
	DefaultConflictEventReadLimit   = 256
	MaxConflictEventReadLimit       = 4096
	MaxConflictEventTextBytes       = 256
	MaxConflictEventSnapshotBytes   = 16 << 20
)

const (
	ConflictDecisionLastWriteWins ConflictEventDecision = iota
	ConflictDecisionSourcePriority
	ConflictDecisionRejected
)

// ConflictEventDecision identifies the policy result recorded for one conflict.
type ConflictEventDecision uint8

func (decision ConflictEventDecision) String() string {
	switch decision {
	case ConflictDecisionLastWriteWins:
		return "last_write_wins"
	case ConflictDecisionSourcePriority:
		return "source_priority"
	case ConflictDecisionRejected:
		return "rejected"
	default:
		return "unknown"
	}
}

// ConflictEventInput contains the metadata needed to record a conflict. Key is
// hashed immediately and is never retained by ConflictEventLog.
type ConflictEventInput struct {
	Space      string
	Key        []byte
	Left       ConflictVersion
	Right      ConflictVersion
	Winner     ConflictVersion
	Decision   ConflictEventDecision
	ObservedAt time.Time
}

// ConflictEvent is a redacted conflict record. KeyDigest is a SHA-256 hex
// digest, not the plaintext key. Versions retain writer identity and ordering
// metadata without retaining values or payload bytes.
type ConflictEvent struct {
	ID           uint64                `json:"id"`
	ObservedAt   time.Time             `json:"observed_at"`
	Space        string                `json:"space"`
	KeyDigest    string                `json:"key_digest"`
	LeftSource   string                `json:"left_source"`
	RightSource  string                `json:"right_source"`
	WinnerSource string                `json:"winner_source,omitempty"`
	Decision     ConflictEventDecision `json:"decision"`
	Left         ConflictVersion       `json:"left"`
	Right        ConflictVersion       `json:"right"`
	Winner       ConflictVersion       `json:"winner,omitempty"`
}

// ConflictEventBatch is a detached cursor read. Pass NextID to Read or Wait
// to continue. HistoryGap means the requested cursor predates retained data.
type ConflictEventBatch struct {
	Events     []ConflictEvent `json:"events"`
	NextID     uint64          `json:"next_id"`
	OldestID   uint64          `json:"oldest_id,omitempty"`
	LatestID   uint64          `json:"latest_id,omitempty"`
	HistoryGap bool            `json:"history_gap,omitempty"`
}

// ConflictEventLogOptions controls the fixed-size in-memory retention window.
// Zero capacity selects DefaultConflictEventLogCapacity.
type ConflictEventLogOptions struct {
	Capacity int
}

// ConflictEventLogSnapshot is a detached, ordered copy suitable for durable
// storage by the caller or for MarshalBinary.
type ConflictEventLogSnapshot struct {
	Capacity int
	NextID   uint64
	Events   []ConflictEvent
}

// ConflictEventLog is a bounded, concurrency-safe redacted conflict stream.
// It retains only the most recent Capacity events and has no background
// goroutine; Wait sleeps on a replaceable notification channel.
type ConflictEventLog struct {
	mu       sync.Mutex
	capacity int
	events   []ConflictEvent
	head     int
	count    int
	nextID   uint64
	wake     chan struct{}
}

// NewConflictEventLog creates an empty bounded conflict event stream.
func NewConflictEventLog(options ConflictEventLogOptions) (*ConflictEventLog, error) {
	capacity := options.Capacity
	if capacity == 0 {
		capacity = DefaultConflictEventLogCapacity
	}
	if capacity < 1 || capacity > MaxConflictEventLogCapacity {
		return nil, fmt.Errorf("%w: got %d, want 1..%d", ErrConflictEventCapacityInvalid, capacity, MaxConflictEventLogCapacity)
	}
	return &ConflictEventLog{
		capacity: capacity,
		events:   make([]ConflictEvent, capacity),
		nextID:   1,
		wake:     make(chan struct{}),
	}, nil
}

// NewConflictEventLogFromSnapshot restores a validated detached snapshot.
func NewConflictEventLogFromSnapshot(snapshot ConflictEventLogSnapshot) (*ConflictEventLog, error) {
	if snapshot.Capacity < 1 || snapshot.Capacity > MaxConflictEventLogCapacity {
		return nil, fmt.Errorf("%w: got %d, want 1..%d", ErrConflictEventCapacityInvalid, snapshot.Capacity, MaxConflictEventLogCapacity)
	}
	if snapshot.NextID == 0 || len(snapshot.Events) > snapshot.Capacity {
		return nil, ErrConflictEventSnapshotInvalid
	}
	if len(snapshot.Events) > 0 {
		first := snapshot.NextID - uint64(len(snapshot.Events))
		if first == 0 {
			return nil, ErrConflictEventSnapshotInvalid
		}
		for index, event := range snapshot.Events {
			if event.ID != first+uint64(index) || event.ID == 0 {
				return nil, ErrConflictEventSnapshotInvalid
			}
			if err := validateConflictEvent(event); err != nil {
				return nil, err
			}
		}
	}
	log, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: snapshot.Capacity})
	if err != nil {
		return nil, err
	}
	log.nextID = snapshot.NextID
	copy(log.events, snapshot.Events)
	log.count = len(snapshot.Events)
	return log, nil
}

// Append records one conflict and returns the detached redacted event.
func (log *ConflictEventLog) Append(input ConflictEventInput) (ConflictEvent, error) {
	if log == nil {
		return ConflictEvent{}, ErrConflictEventLogNil
	}
	if log.capacity < 1 || len(log.events) != log.capacity || log.wake == nil {
		return ConflictEvent{}, ErrConflictEventCapacityInvalid
	}
	event, err := normalizeConflictEventInput(input)
	if err != nil {
		return ConflictEvent{}, err
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.nextID == 0 || log.nextID == ^uint64(0) {
		return ConflictEvent{}, ErrConflictEventIDExhausted
	}
	event.ID = log.nextID
	log.nextID++
	if log.count < log.capacity {
		index := (log.head + log.count) % log.capacity
		log.events[index] = event
		log.count++
	} else {
		log.events[log.head] = event
		log.head = (log.head + 1) % log.capacity
	}
	close(log.wake)
	log.wake = make(chan struct{})
	return event, nil
}

// Read returns up to limit events after afterID. A non-positive limit selects
// DefaultConflictEventReadLimit and oversized limits are capped.
func (log *ConflictEventLog) Read(afterID uint64, limit int) ConflictEventBatch {
	if log == nil {
		return ConflictEventBatch{}
	}
	limit = normalizeConflictEventReadLimit(limit)
	log.mu.Lock()
	defer log.mu.Unlock()
	return log.readLocked(afterID, limit)
}

// Wait returns the next available batch, waiting without a per-client
// goroutine until an event arrives or ctx is canceled.
func (log *ConflictEventLog) Wait(ctx context.Context, afterID uint64, limit int) (ConflictEventBatch, error) {
	if log == nil {
		return ConflictEventBatch{}, ErrConflictEventLogNil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	limit = normalizeConflictEventReadLimit(limit)
	for {
		if err := ctx.Err(); err != nil {
			return ConflictEventBatch{}, err
		}
		log.mu.Lock()
		batch := log.readLocked(afterID, limit)
		if batch.HistoryGap || len(batch.Events) > 0 {
			log.mu.Unlock()
			return batch, nil
		}
		wake := log.wake
		log.mu.Unlock()
		select {
		case <-ctx.Done():
			return ConflictEventBatch{}, ctx.Err()
		case <-wake:
		}
	}
}

// Snapshot returns a detached, oldest-to-newest copy of retained events.
func (log *ConflictEventLog) Snapshot() ConflictEventLogSnapshot {
	if log == nil {
		return ConflictEventLogSnapshot{}
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	return log.snapshotLocked()
}

// MarshalBinary returns a checksummed HCI1 snapshot. It contains no plaintext
// key bytes and is bounded by MaxConflictEventSnapshotBytes.
func (log *ConflictEventLog) MarshalBinary() ([]byte, error) {
	if log == nil {
		return nil, ErrConflictEventLogNil
	}
	return marshalConflictEventSnapshot(log.Snapshot())
}

// NewConflictEventLogFromBinary restores a checksummed HCI1 snapshot.
func NewConflictEventLogFromBinary(data []byte) (*ConflictEventLog, error) {
	snapshot, err := unmarshalConflictEventSnapshot(data)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrConflictEventSnapshotCorrupt, err)
	}
	log, err := NewConflictEventLogFromSnapshot(snapshot)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrConflictEventSnapshotCorrupt, err)
	}
	return log, nil
}

func (log *ConflictEventLog) readLocked(afterID uint64, limit int) ConflictEventBatch {
	batch := ConflictEventBatch{NextID: afterID}
	if log.count == 0 {
		return batch
	}
	oldest := log.events[log.head].ID
	latest := log.events[(log.head+log.count-1)%log.capacity].ID
	batch.OldestID = oldest
	batch.LatestID = latest
	batch.HistoryGap = oldest > 1 && afterID < oldest-1
	start := 0
	if afterID >= oldest {
		if afterID >= latest {
			return batch
		}
		start = int(afterID - oldest + 1)
	}
	remaining := log.count - start
	if remaining > limit {
		remaining = limit
	}
	batch.Events = make([]ConflictEvent, 0, remaining)
	for index := 0; index < remaining; index++ {
		event := log.events[(log.head+start+index)%log.capacity]
		batch.Events = append(batch.Events, event)
		batch.NextID = event.ID
	}
	return batch
}

func (log *ConflictEventLog) snapshotLocked() ConflictEventLogSnapshot {
	snapshot := ConflictEventLogSnapshot{
		Capacity: log.capacity,
		NextID:   log.nextID,
		Events:   make([]ConflictEvent, 0, log.count),
	}
	for index := 0; index < log.count; index++ {
		snapshot.Events = append(snapshot.Events, log.events[(log.head+index)%log.capacity])
	}
	return snapshot
}

func normalizeConflictEventReadLimit(limit int) int {
	if limit <= 0 {
		return DefaultConflictEventReadLimit
	}
	if limit > MaxConflictEventReadLimit {
		return MaxConflictEventReadLimit
	}
	return limit
}

func normalizeConflictEventInput(input ConflictEventInput) (ConflictEvent, error) {
	space := strings.TrimSpace(input.Space)
	if space == "" {
		return ConflictEvent{}, ErrConflictEventSpaceRequired
	}
	if len(space) > MaxConflictEventTextBytes {
		return ConflictEvent{}, fmt.Errorf("%w: space exceeds %d bytes", ErrConflictEventSpaceRequired, MaxConflictEventTextBytes)
	}
	if err := validateConflictVersions(input.Left, input.Right); err != nil {
		return ConflictEvent{}, err
	}
	switch input.Decision {
	case ConflictDecisionLastWriteWins, ConflictDecisionSourcePriority:
		if input.Winner != input.Left && input.Winner != input.Right {
			return ConflictEvent{}, fmt.Errorf("%w: winner must be one of the conflicting versions", ErrConflictEventVersionInvalid)
		}
	case ConflictDecisionRejected:
		if input.Winner != (ConflictVersion{}) {
			return ConflictEvent{}, fmt.Errorf("%w: rejected conflict cannot have a winner", ErrConflictEventVersionInvalid)
		}
	default:
		return ConflictEvent{}, fmt.Errorf("%w: %d", ErrConflictEventDecisionInvalid, input.Decision)
	}
	observedAt := input.ObservedAt
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	} else {
		observedAt = observedAt.UTC()
	}
	digest := sha256.Sum256(input.Key)
	return ConflictEvent{
		ObservedAt:   observedAt,
		Space:        space,
		KeyDigest:    hex.EncodeToString(digest[:]),
		LeftSource:   input.Left.NodeID,
		RightSource:  input.Right.NodeID,
		WinnerSource: input.Winner.NodeID,
		Decision:     input.Decision,
		Left:         input.Left,
		Right:        input.Right,
		Winner:       input.Winner,
	}, nil
}

func validateConflictVersions(left, right ConflictVersion) error {
	if len(left.NodeID) == 0 || len(right.NodeID) == 0 || len(left.NodeID) > MaxConflictEventTextBytes || len(right.NodeID) > MaxConflictEventTextBytes {
		return ErrConflictEventVersionInvalid
	}
	if _, err := CompareConflictVersions(left, right); err != nil {
		return fmt.Errorf("%w: %v", ErrConflictEventVersionInvalid, err)
	}
	return nil
}

func validateConflictEvent(event ConflictEvent) error {
	if event.ID == 0 || len(event.Space) == 0 || len(event.Space) > MaxConflictEventTextBytes {
		return ErrConflictEventSnapshotInvalid
	}
	if len(event.KeyDigest) != sha256.Size*2 {
		return ErrConflictEventSnapshotInvalid
	}
	if _, err := hex.DecodeString(event.KeyDigest); err != nil {
		return ErrConflictEventSnapshotInvalid
	}
	if err := validateConflictVersions(event.Left, event.Right); err != nil {
		return ErrConflictEventSnapshotInvalid
	}
	if event.LeftSource != event.Left.NodeID || event.RightSource != event.Right.NodeID || len(event.WinnerSource) > MaxConflictEventTextBytes {
		return ErrConflictEventSnapshotInvalid
	}
	switch event.Decision {
	case ConflictDecisionLastWriteWins, ConflictDecisionSourcePriority:
		if event.Winner != event.Left && event.Winner != event.Right || event.WinnerSource != event.Winner.NodeID {
			return ErrConflictEventSnapshotInvalid
		}
	case ConflictDecisionRejected:
		if event.Winner != (ConflictVersion{}) || event.WinnerSource != "" {
			return ErrConflictEventSnapshotInvalid
		}
	default:
		return ErrConflictEventSnapshotInvalid
	}
	return nil
}

const (
	conflictEventSnapshotHeaderBytes  = 9
	conflictEventSnapshotTrailerBytes = 4
)

var conflictEventSnapshotCRCTable = crc32.MakeTable(crc32.Castagnoli)

func marshalConflictEventSnapshot(snapshot ConflictEventLogSnapshot) ([]byte, error) {
	if snapshot.NextID == 0 || snapshot.Capacity < 1 || len(snapshot.Events) > snapshot.Capacity {
		return nil, ErrConflictEventSnapshotInvalid
	}
	for _, event := range snapshot.Events {
		if err := validateConflictEvent(event); err != nil {
			return nil, err
		}
	}
	payload := make([]byte, 0, 32+len(snapshot.Events)*192)
	payload = appendConflictEventUint32(payload, uint32(snapshot.Capacity))
	payload = appendConflictEventUint64(payload, snapshot.NextID)
	payload = appendConflictEventUint32(payload, uint32(len(snapshot.Events)))
	for _, event := range snapshot.Events {
		payload = appendConflictEventUint64(payload, event.ID)
		payload = appendConflictEventUint64(payload, uint64(event.ObservedAt.UnixNano()))
		payload = append(payload, byte(event.Decision))
		var err error
		for _, value := range []string{event.Space, event.KeyDigest, event.LeftSource, event.RightSource, event.WinnerSource} {
			payload, err = appendConflictEventString(payload, value)
			if err != nil {
				return nil, err
			}
		}
		payload = appendConflictEventVersion(payload, event.Left)
		payload = appendConflictEventVersion(payload, event.Right)
		payload = appendConflictEventVersion(payload, event.Winner)
	}
	if len(payload) > MaxConflictEventSnapshotBytes {
		return nil, fmt.Errorf("%w: payload exceeds %d bytes", ErrConflictEventSnapshotInvalid, MaxConflictEventSnapshotBytes)
	}
	data := make([]byte, 0, conflictEventSnapshotHeaderBytes+len(payload)+conflictEventSnapshotTrailerBytes)
	data = append(data, 'H', 'C', 'I', '1', 1)
	data = appendConflictEventUint32(data, uint32(len(payload)))
	data = append(data, payload...)
	data = appendConflictEventUint32(data, crc32.Checksum(payload, conflictEventSnapshotCRCTable))
	return data, nil
}

func unmarshalConflictEventSnapshot(data []byte) (ConflictEventLogSnapshot, error) {
	if len(data) < conflictEventSnapshotHeaderBytes+conflictEventSnapshotTrailerBytes || string(data[:4]) != "HCI1" || data[4] != 1 {
		return ConflictEventLogSnapshot{}, ErrConflictEventSnapshotInvalid
	}
	payloadLength := binary.LittleEndian.Uint32(data[5:9])
	if payloadLength > MaxConflictEventSnapshotBytes || int(payloadLength)+conflictEventSnapshotHeaderBytes+conflictEventSnapshotTrailerBytes != len(data) {
		return ConflictEventLogSnapshot{}, ErrConflictEventSnapshotInvalid
	}
	payloadEnd := conflictEventSnapshotHeaderBytes + int(payloadLength)
	payload := data[conflictEventSnapshotHeaderBytes:payloadEnd]
	if crc32.Checksum(payload, conflictEventSnapshotCRCTable) != binary.LittleEndian.Uint32(data[payloadEnd:]) {
		return ConflictEventLogSnapshot{}, ErrConflictEventSnapshotCorrupt
	}
	offset := 0
	capacity, err := readConflictEventUint32(payload, &offset)
	if err != nil {
		return ConflictEventLogSnapshot{}, err
	}
	nextID, err := readConflictEventUint64(payload, &offset)
	if err != nil {
		return ConflictEventLogSnapshot{}, err
	}
	count, err := readConflictEventUint32(payload, &offset)
	if err != nil || count > MaxConflictEventLogCapacity {
		return ConflictEventLogSnapshot{}, ErrConflictEventSnapshotInvalid
	}
	snapshot := ConflictEventLogSnapshot{Capacity: int(capacity), NextID: nextID, Events: make([]ConflictEvent, 0, int(count))}
	for index := uint32(0); index < count; index++ {
		id, readErr := readConflictEventUint64(payload, &offset)
		if readErr != nil {
			return ConflictEventLogSnapshot{}, readErr
		}
		observedNanos, readErr := readConflictEventUint64(payload, &offset)
		if readErr != nil || offset >= len(payload) {
			return ConflictEventLogSnapshot{}, ErrConflictEventSnapshotInvalid
		}
		decision := ConflictEventDecision(payload[offset])
		offset++
		values := make([]string, 5)
		for valueIndex := range values {
			values[valueIndex], readErr = readConflictEventString(payload, &offset, MaxConflictEventTextBytes*2)
			if readErr != nil {
				return ConflictEventLogSnapshot{}, readErr
			}
		}
		left, readErr := readConflictEventVersion(payload, &offset)
		if readErr != nil {
			return ConflictEventLogSnapshot{}, readErr
		}
		right, readErr := readConflictEventVersion(payload, &offset)
		if readErr != nil {
			return ConflictEventLogSnapshot{}, readErr
		}
		winner, readErr := readConflictEventVersion(payload, &offset)
		if readErr != nil {
			return ConflictEventLogSnapshot{}, readErr
		}
		event := ConflictEvent{
			ID:           id,
			ObservedAt:   time.Unix(0, int64(observedNanos)).UTC(),
			Space:        values[0],
			KeyDigest:    values[1],
			LeftSource:   values[2],
			RightSource:  values[3],
			WinnerSource: values[4],
			Decision:     decision,
			Left:         left,
			Right:        right,
			Winner:       winner,
		}
		snapshot.Events = append(snapshot.Events, event)
	}
	if offset != len(payload) {
		return ConflictEventLogSnapshot{}, ErrConflictEventSnapshotInvalid
	}
	return snapshot, nil
}

func appendConflictEventUint32(data []byte, value uint32) []byte {
	var encoded [4]byte
	binary.LittleEndian.PutUint32(encoded[:], value)
	return append(data, encoded[:]...)
}

func appendConflictEventUint64(data []byte, value uint64) []byte {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], value)
	return append(data, encoded[:]...)
}

func appendConflictEventString(data []byte, value string) ([]byte, error) {
	if len(value) > int(^uint16(0)) {
		return nil, ErrConflictEventSnapshotInvalid
	}
	var encoded [2]byte
	binary.LittleEndian.PutUint16(encoded[:], uint16(len(value)))
	data = append(data, encoded[:]...)
	return append(data, value...), nil
}

func appendConflictEventVersion(data []byte, version ConflictVersion) []byte {
	data = appendConflictEventUint64(data, uint64(version.Timestamp))
	data = appendConflictEventUint64(data, version.Sequence)
	data, _ = appendConflictEventString(data, version.NodeID)
	return data
}

func readConflictEventUint32(data []byte, offset *int) (uint32, error) {
	if *offset < 0 || len(data)-*offset < 4 {
		return 0, ErrConflictEventSnapshotInvalid
	}
	value := binary.LittleEndian.Uint32(data[*offset : *offset+4])
	*offset += 4
	return value, nil
}

func readConflictEventUint64(data []byte, offset *int) (uint64, error) {
	if *offset < 0 || len(data)-*offset < 8 {
		return 0, ErrConflictEventSnapshotInvalid
	}
	value := binary.LittleEndian.Uint64(data[*offset : *offset+8])
	*offset += 8
	return value, nil
}

func readConflictEventString(data []byte, offset *int, max int) (string, error) {
	if *offset < 0 || len(data)-*offset < 2 {
		return "", ErrConflictEventSnapshotInvalid
	}
	length := int(binary.LittleEndian.Uint16(data[*offset : *offset+2]))
	*offset += 2
	if length > max || len(data)-*offset < length {
		return "", ErrConflictEventSnapshotInvalid
	}
	value := string(data[*offset : *offset+length])
	*offset += length
	return value, nil
}

func readConflictEventVersion(data []byte, offset *int) (ConflictVersion, error) {
	timestamp, err := readConflictEventUint64(data, offset)
	if err != nil {
		return ConflictVersion{}, err
	}
	sequence, err := readConflictEventUint64(data, offset)
	if err != nil {
		return ConflictVersion{}, err
	}
	nodeID, err := readConflictEventString(data, offset, MaxConflictEventTextBytes)
	if err != nil {
		return ConflictVersion{}, err
	}
	return ConflictVersion{Timestamp: int64(timestamp), NodeID: nodeID, Sequence: sequence}, nil
}
