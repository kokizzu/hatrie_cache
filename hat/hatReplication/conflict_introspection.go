package hatReplication

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
)

const (
	// DefaultConflictInspectionCapacity is a bounded, operationally conservative
	// starting point for callers that want a short in-memory conflict history.
	DefaultConflictInspectionCapacity = 4096
	// MaxConflictInspectionCapacity prevents an accidental unbounded allocation.
	MaxConflictInspectionCapacity = 65536
	// MaxConflictInspectionPageSize bounds one read response.
	MaxConflictInspectionPageSize = 1024
)

var (
	ErrConflictInspectionLogNil            = errors.New("hatriecache: conflict inspection log is nil")
	ErrConflictInspectionCapacityInvalid   = errors.New("hatriecache: conflict inspection capacity is invalid")
	ErrConflictInspectionSpaceRequired     = errors.New("hatriecache: conflict inspection space is required")
	ErrConflictInspectionKeyRequired       = errors.New("hatriecache: conflict inspection key is required")
	ErrConflictInspectionPolicyInvalid     = errors.New("hatriecache: conflict inspection policy is invalid")
	ErrConflictInspectionDecisionInvalid   = errors.New("hatriecache: conflict inspection decision is invalid")
	ErrConflictInspectionSequenceExhausted = errors.New("hatriecache: conflict inspection sequence is exhausted")
)

// ConflictInspectionDecision describes the outcome recorded for one conflict.
type ConflictInspectionDecision string

const (
	ConflictInspectionWinner   ConflictInspectionDecision = "winner"
	ConflictInspectionRejected ConflictInspectionDecision = "rejected"
)

// ConflictInspectionEvent is a redacted conflict record. KeyDigest is the
// SHA-256 hex digest of the application key; the raw key is never retained.
type ConflictInspectionEvent struct {
	Sequence  uint64                     `json:"sequence"`
	Space     string                     `json:"space"`
	KeyDigest string                     `json:"key_digest"`
	Left      ConflictVersion            `json:"left"`
	Right     ConflictVersion            `json:"right"`
	Winner    ConflictVersion            `json:"winner"`
	Policy    ConflictPolicyMode         `json:"policy"`
	Decision  ConflictInspectionDecision `json:"decision"`
}

// ConflictInspectionPage is one bounded cursor read from a log. Pass
// NextSequence as afterSequence to continue polling. Truncated means at least
// one older event was overwritten before this read.
type ConflictInspectionPage struct {
	Events         []ConflictInspectionEvent `json:"events"`
	OldestSequence uint64                    `json:"oldest_sequence"`
	NextSequence   uint64                    `json:"next_sequence"`
	Truncated      bool                      `json:"truncated"`
}

// ConflictInspectionLog stores a bounded, thread-safe ring of conflict events.
// It is intentionally not attached to ConflictPolicyRegistry, so the default
// conflict-resolution path pays no logging or allocation cost.
type ConflictInspectionLog struct {
	mu           sync.Mutex
	capacity     int
	nextSequence uint64
	start        int
	size         int
	events       []ConflictInspectionEvent
}

// NewConflictInspectionLog creates a fixed-capacity in-memory conflict log.
func NewConflictInspectionLog(capacity int) (*ConflictInspectionLog, error) {
	if capacity <= 0 || capacity > MaxConflictInspectionCapacity {
		return nil, fmt.Errorf("%w: want 1..%d, got %d", ErrConflictInspectionCapacityInvalid, MaxConflictInspectionCapacity, capacity)
	}
	return &ConflictInspectionLog{
		capacity:     capacity,
		nextSequence: 1,
		events:       make([]ConflictInspectionEvent, capacity),
	}, nil
}

// Record appends one redacted event, overwriting the oldest event at capacity.
func (log *ConflictInspectionLog) Record(space, key string, policy ConflictPolicyMode, left, right, winner ConflictVersion, decision ConflictInspectionDecision) (ConflictInspectionEvent, error) {
	if log == nil {
		return ConflictInspectionEvent{}, ErrConflictInspectionLogNil
	}
	space = strings.TrimSpace(space)
	if space == "" {
		return ConflictInspectionEvent{}, ErrConflictInspectionSpaceRequired
	}
	if key == "" {
		return ConflictInspectionEvent{}, ErrConflictInspectionKeyRequired
	}
	if !validConflictInspectionPolicy(policy) {
		return ConflictInspectionEvent{}, ErrConflictInspectionPolicyInvalid
	}
	if !validConflictInspectionDecision(decision) {
		return ConflictInspectionEvent{}, ErrConflictInspectionDecisionInvalid
	}
	if _, err := CompareConflictVersions(left, right); err != nil {
		return ConflictInspectionEvent{}, err
	}
	if decision == ConflictInspectionWinner {
		if _, err := CompareConflictVersions(winner, winner); err != nil || (winner != left && winner != right) {
			return ConflictInspectionEvent{}, fmt.Errorf("%w: winner must be one of the conflicting versions", ErrConflictInspectionDecisionInvalid)
		}
	} else if winner != (ConflictVersion{}) {
		return ConflictInspectionEvent{}, fmt.Errorf("%w: rejected conflicts cannot have a winner", ErrConflictInspectionDecisionInvalid)
	}

	digest := sha256.Sum256([]byte(key))
	event := ConflictInspectionEvent{
		Space:     space,
		KeyDigest: hex.EncodeToString(digest[:]),
		Left:      left,
		Right:     right,
		Winner:    winner,
		Policy:    policy,
		Decision:  decision,
	}

	log.mu.Lock()
	defer log.mu.Unlock()
	if log.nextSequence == 0 {
		return ConflictInspectionEvent{}, ErrConflictInspectionSequenceExhausted
	}
	event.Sequence = log.nextSequence
	log.nextSequence++
	index := (log.start + log.size) % log.capacity
	if log.size == log.capacity {
		index = log.start
		log.start = (log.start + 1) % log.capacity
	} else {
		log.size++
	}
	log.events[index] = event
	return event, nil
}

// Snapshot returns all retained events in ascending sequence order.
func (log *ConflictInspectionLog) Snapshot() []ConflictInspectionEvent {
	if log == nil {
		return nil
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	return log.snapshotLocked()
}

// Read returns events after afterSequence in ascending order. A non-positive
// limit uses MaxConflictInspectionPageSize, and larger limits are capped.
func (log *ConflictInspectionLog) Read(afterSequence uint64, limit int) ConflictInspectionPage {
	if log == nil {
		return ConflictInspectionPage{NextSequence: afterSequence}
	}
	if limit <= 0 || limit > MaxConflictInspectionPageSize {
		limit = MaxConflictInspectionPageSize
	}

	log.mu.Lock()
	defer log.mu.Unlock()
	page := ConflictInspectionPage{NextSequence: afterSequence}
	if log.size == 0 {
		return page
	}
	page.OldestSequence = log.nextSequence - uint64(log.size)
	latestSequence := log.nextSequence - 1
	page.Truncated = afterSequence < page.OldestSequence-1
	if afterSequence >= latestSequence {
		return page
	}
	firstSequence := afterSequence + 1
	if firstSequence < page.OldestSequence {
		firstSequence = page.OldestSequence
	}
	count := latestSequence - firstSequence + 1
	if count > uint64(limit) {
		count = uint64(limit)
	}
	page.Events = make([]ConflictInspectionEvent, int(count))
	for index := range page.Events {
		sequence := firstSequence + uint64(index)
		ringIndex := (log.start + int(sequence-page.OldestSequence)) % log.capacity
		page.Events[index] = log.events[ringIndex]
	}
	page.NextSequence = page.Events[len(page.Events)-1].Sequence
	return page
}

// ResolveAndRecord applies the configured policy and records the result in an
// optional redacted log. A nil log preserves the normal Resolve behavior.
func (registry *ConflictPolicyRegistry) ResolveAndRecord(log *ConflictInspectionLog, space, key string, left, right ConflictVersion) (ConflictVersion, error) {
	if log == nil {
		return registry.Resolve(space, left, right)
	}
	policy, err := registry.conflictInspectionPolicy(space)
	if err != nil {
		return ConflictVersion{}, err
	}
	winner, resolveErr := resolveConflictWithPolicy(policy, left, right)
	decision := ConflictInspectionWinner
	if errors.Is(resolveErr, ErrConflictRejected) {
		decision = ConflictInspectionRejected
		winner = ConflictVersion{}
	} else if resolveErr != nil {
		return ConflictVersion{}, resolveErr
	}
	if _, err := log.Record(space, key, policy.Mode, left, right, winner, decision); err != nil {
		return ConflictVersion{}, err
	}
	if resolveErr != nil {
		return ConflictVersion{}, resolveErr
	}
	return winner, nil
}

func (log *ConflictInspectionLog) snapshotLocked() []ConflictInspectionEvent {
	events := make([]ConflictInspectionEvent, log.size)
	if log.size == 0 {
		return events
	}
	oldestSequence := log.nextSequence - uint64(log.size)
	for index := range events {
		sequence := oldestSequence + uint64(index)
		ringIndex := (log.start + index) % log.capacity
		if log.events[ringIndex].Sequence != sequence {
			panic("hatriecache: conflict inspection ring sequence corruption")
		}
		events[index] = log.events[ringIndex]
	}
	return events
}

func (registry *ConflictPolicyRegistry) conflictInspectionPolicy(space string) (ConflictPolicy, error) {
	if registry == nil {
		return ConflictPolicy{}, ErrConflictPolicyRegistryNil
	}
	space = strings.TrimSpace(space)
	if space == "" {
		return ConflictPolicy{}, ErrConflictPolicySpaceRequired
	}
	registry.mu.RLock()
	policy, exists := registry.spaceOverrides[space]
	if !exists {
		policy = registry.defaultPolicy
	}
	registry.mu.RUnlock()
	return policy, nil
}

func validConflictInspectionPolicy(policy ConflictPolicyMode) bool {
	return policy == ConflictPolicyLastWriteWins || policy == ConflictPolicySourcePriority || policy == ConflictPolicyReject
}

func validConflictInspectionDecision(decision ConflictInspectionDecision) bool {
	return decision == ConflictInspectionWinner || decision == ConflictInspectionRejected
}
