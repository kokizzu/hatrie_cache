package hatReplication

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
)

const (
	// DefaultConflictEventLogCapacity is used when a caller passes zero to the
	// constructor.
	DefaultConflictEventLogCapacity = 1024
	// MaxConflictEventLogCapacity bounds retained conflict metadata.
	MaxConflictEventLogCapacity = 1 << 20
	// MaxConflictEventSpaceBytes bounds one redacted space name.
	MaxConflictEventSpaceBytes = 256
	// DefaultConflictEventReadLimit bounds a read that does not specify a limit.
	DefaultConflictEventReadLimit = 256
	// MaxConflictEventReadLimit bounds one replay batch.
	MaxConflictEventReadLimit = 4096
)

var (
	ErrConflictEventInvalid          = errors.New("hatriecache: conflict event is invalid")
	ErrConflictEventLogNil           = errors.New("hatriecache: conflict event log is nil")
	ErrConflictEventCursorStale      = errors.New("hatriecache: conflict event cursor is stale")
	ErrConflictEventSnapshotInvalid  = errors.New("hatriecache: conflict event snapshot is invalid")
	ErrConflictEventSequenceOverflow = errors.New("hatriecache: conflict event sequence overflow")
)

// ConflictEventDecision records the result of one conflict policy decision.
type ConflictEventDecision uint8

const (
	ConflictEventDecisionLeft ConflictEventDecision = iota + 1
	ConflictEventDecisionRight
	ConflictEventDecisionRejected
)

// ConflictEvent is redacted conflict metadata. KeyDigest must be a caller-
// supplied digest; the log never accepts or retains the original key bytes.
type ConflictEvent struct {
	Sequence  uint64                `json:"sequence"`
	Space     string                `json:"space"`
	KeyDigest [16]byte              `json:"key_digest"`
	Left      ConflictVersion       `json:"left"`
	Right     ConflictVersion       `json:"right"`
	Winner    ConflictVersion       `json:"winner"`
	Decision  ConflictEventDecision `json:"decision"`
	Policy    ConflictPolicyMode    `json:"policy"`
}

// ConflictEventLogSnapshot is a deterministic, copy-safe representation of a
// conflict event log. Events are ordered from oldest retained to newest.
type ConflictEventLogSnapshot struct {
	Capacity     int             `json:"capacity"`
	LastSequence uint64          `json:"last_sequence"`
	Events       []ConflictEvent `json:"events"`
}

// ConflictEventLog is an opt-in bounded replay log. It is independent of the
// conflict application path so callers can persist or expose it only when
// conflict diagnostics are required.
type ConflictEventLog struct {
	mu           sync.RWMutex
	capacity     int
	events       []ConflictEvent
	start        int
	count        int
	lastSequence uint64
}

// NewConflictEventLog creates a bounded conflict log. A zero capacity selects
// DefaultConflictEventLogCapacity.
func NewConflictEventLog(capacity int) (*ConflictEventLog, error) {
	if capacity == 0 {
		capacity = DefaultConflictEventLogCapacity
	}
	if capacity < 1 || capacity > MaxConflictEventLogCapacity {
		return nil, fmt.Errorf("%w: capacity must be 1..%d", ErrConflictEventInvalid, MaxConflictEventLogCapacity)
	}
	return &ConflictEventLog{
		capacity: capacity,
		events:   make([]ConflictEvent, capacity),
	}, nil
}

// Append adds one validated redacted event and returns its monotone cursor.
// The supplied Sequence is ignored only when it is zero; callers cannot choose
// or rewind the log sequence.
func (log *ConflictEventLog) Append(event ConflictEvent) (uint64, error) {
	if log == nil {
		return 0, ErrConflictEventLogNil
	}
	if event.Sequence != 0 {
		return 0, ErrConflictEventInvalid
	}
	normalized, err := normalizeConflictEvent(event, false)
	if err != nil {
		return 0, err
	}

	log.mu.Lock()
	defer log.mu.Unlock()
	if log.lastSequence == math.MaxUint64 {
		return 0, ErrConflictEventSequenceOverflow
	}
	sequence := log.lastSequence + 1
	normalized.Sequence = sequence
	index := (log.start + log.count) % log.capacity
	if log.count == log.capacity {
		index = log.start
		log.start = (log.start + 1) % log.capacity
	} else {
		log.count++
	}
	log.events[index] = normalized
	log.lastSequence = sequence
	return sequence, nil
}

// Read returns events strictly after after. Cursor zero starts at the oldest
// retained event. A nonzero cursor older than the retained window is stale.
// A zero limit selects DefaultConflictEventReadLimit.
func (log *ConflictEventLog) Read(after uint64, limit int) ([]ConflictEvent, uint64, error) {
	if log == nil {
		return nil, after, ErrConflictEventLogNil
	}
	if limit == 0 {
		limit = DefaultConflictEventReadLimit
	}
	if limit < 0 || limit > MaxConflictEventReadLimit {
		return nil, after, fmt.Errorf("%w: read limit must be 1..%d", ErrConflictEventInvalid, MaxConflictEventReadLimit)
	}

	log.mu.RLock()
	defer log.mu.RUnlock()
	if log.count == 0 || after >= log.lastSequence {
		return nil, after, nil
	}
	oldest := log.events[log.start].Sequence
	if after != 0 && after < oldest-1 {
		return nil, after, ErrConflictEventCursorStale
	}
	first := oldest
	if after >= oldest {
		first = after + 1
	}
	offset := int(first - oldest)
	if offset >= log.count {
		return nil, after, nil
	}
	count := log.count - offset
	if count > limit {
		count = limit
	}
	result := make([]ConflictEvent, count)
	for index := range result {
		result[index] = log.events[(log.start+offset+index)%log.capacity]
	}
	return result, result[len(result)-1].Sequence, nil
}

// Snapshot returns a copy-safe snapshot in replay order.
func (log *ConflictEventLog) Snapshot() ConflictEventLogSnapshot {
	if log == nil {
		return ConflictEventLogSnapshot{}
	}
	log.mu.RLock()
	defer log.mu.RUnlock()
	events := make([]ConflictEvent, log.count)
	for index := range events {
		events[index] = log.events[(log.start+index)%log.capacity]
	}
	return ConflictEventLogSnapshot{
		Capacity:     log.capacity,
		LastSequence: log.lastSequence,
		Events:       events,
	}
}

// NewConflictEventLogFromSnapshot restores a validated conflict event log.
func NewConflictEventLogFromSnapshot(snapshot ConflictEventLogSnapshot) (*ConflictEventLog, error) {
	if snapshot.Capacity < 1 || snapshot.Capacity > MaxConflictEventLogCapacity || len(snapshot.Events) > snapshot.Capacity {
		return nil, fmt.Errorf("%w: capacity or event count is invalid", ErrConflictEventSnapshotInvalid)
	}
	if snapshot.LastSequence == 0 && len(snapshot.Events) != 0 {
		return nil, fmt.Errorf("%w: nonempty events require a sequence", ErrConflictEventSnapshotInvalid)
	}
	if len(snapshot.Events) == 0 && snapshot.LastSequence != 0 {
		return nil, fmt.Errorf("%w: missing retained event window", ErrConflictEventSnapshotInvalid)
	}
	if len(snapshot.Events) > 0 {
		first := snapshot.LastSequence - uint64(len(snapshot.Events)) + 1
		if first == 0 {
			return nil, fmt.Errorf("%w: sequence underflow", ErrConflictEventSnapshotInvalid)
		}
		for index, event := range snapshot.Events {
			want := first + uint64(index)
			if event.Sequence != want {
				return nil, fmt.Errorf("%w: event sequence %d, want %d", ErrConflictEventSnapshotInvalid, event.Sequence, want)
			}
			if _, err := normalizeConflictEvent(event, true); err != nil {
				return nil, fmt.Errorf("%w: %v", ErrConflictEventSnapshotInvalid, err)
			}
		}
	}
	log := &ConflictEventLog{
		capacity:     snapshot.Capacity,
		events:       make([]ConflictEvent, snapshot.Capacity),
		count:        len(snapshot.Events),
		lastSequence: snapshot.LastSequence,
	}
	copy(log.events, snapshot.Events)
	return log, nil
}

func normalizeConflictEvent(event ConflictEvent, requireSequence bool) (ConflictEvent, error) {
	if requireSequence {
		if event.Sequence == 0 {
			return ConflictEvent{}, fmt.Errorf("%w: sequence is required", ErrConflictEventInvalid)
		}
	} else if event.Sequence != 0 {
		return ConflictEvent{}, ErrConflictEventInvalid
	}
	event.Space = strings.TrimSpace(event.Space)
	if event.Space == "" || len(event.Space) > MaxConflictEventSpaceBytes {
		return ConflictEvent{}, fmt.Errorf("%w: space is empty or too long", ErrConflictEventInvalid)
	}
	if event.Policy > ConflictPolicyReject {
		return ConflictEvent{}, fmt.Errorf("%w: unsupported policy %d", ErrConflictEventInvalid, event.Policy)
	}
	if _, err := CompareConflictVersions(event.Left, event.Right); err != nil {
		return ConflictEvent{}, fmt.Errorf("%w: versions: %v", ErrConflictEventInvalid, err)
	}
	switch event.Decision {
	case ConflictEventDecisionLeft:
		if event.Winner != event.Left {
			return ConflictEvent{}, fmt.Errorf("%w: left decision has a different winner", ErrConflictEventInvalid)
		}
	case ConflictEventDecisionRight:
		if event.Winner != event.Right {
			return ConflictEvent{}, fmt.Errorf("%w: right decision has a different winner", ErrConflictEventInvalid)
		}
	case ConflictEventDecisionRejected:
		if event.Winner != (ConflictVersion{}) {
			return ConflictEvent{}, fmt.Errorf("%w: rejected decision cannot have a winner", ErrConflictEventInvalid)
		}
	default:
		return ConflictEvent{}, fmt.Errorf("%w: unsupported decision %d", ErrConflictEventInvalid, event.Decision)
	}
	return event, nil
}
