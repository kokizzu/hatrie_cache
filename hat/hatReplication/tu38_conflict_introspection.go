package hatReplication

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrConflictEventCapacityInvalid reports an unsupported bounded history size.
	ErrConflictEventCapacityInvalid = errors.New("hatriecache: conflict event capacity is invalid")
	// ErrConflictEventCursorExpired reports that the requested history was evicted.
	ErrConflictEventCursorExpired = errors.New("hatriecache: conflict event cursor expired")
	// ErrConflictEventLogClosed reports that a waiter cannot observe more events.
	ErrConflictEventLogClosed = errors.New("hatriecache: conflict event log is closed")
)

const (
	// DefaultConflictEventCapacity is used when an observer requests zero capacity.
	DefaultConflictEventCapacity = 1024
	// MaxConflictEventCapacity bounds retained conflict metadata and waiter state.
	MaxConflictEventCapacity = 65536
)

// ConflictEventOutcome is the redacted result of one policy resolution.
type ConflictEventOutcome string

const (
	ConflictEventResolved ConflictEventOutcome = "resolved"
	ConflictEventRejected ConflictEventOutcome = "rejected"
	ConflictEventError    ConflictEventOutcome = "error"
)

// ConflictEvent contains conflict metadata but never contains a key or value.
// Node IDs and version coordinates are retained so operators can explain a
// decision without exposing application payloads.
type ConflictEvent struct {
	Sequence uint64               `json:"sequence"`
	Space    string               `json:"space"`
	Mode     ConflictPolicyMode   `json:"mode"`
	Outcome  ConflictEventOutcome `json:"outcome"`
	Left     ConflictVersion      `json:"left"`
	Right    ConflictVersion      `json:"right"`
	Winner   ConflictVersion      `json:"winner"`
}

// ConflictEventLogStats is an allocation-free bounded-history snapshot.
type ConflictEventLogStats struct {
	Capacity      int    `json:"capacity"`
	Retained      int    `json:"retained"`
	Dropped       uint64 `json:"dropped"`
	FirstSequence uint64 `json:"first_sequence"`
	LastSequence  uint64 `json:"last_sequence"`
}

// ConflictEventLog is a bounded cursor stream for redacted conflict metadata.
// It retains no payloads and starts no background goroutine. A nil log on a
// ConflictPolicyRegistry keeps the ordinary resolution path disabled.
type ConflictEventLog struct {
	mu           sync.Mutex
	capacity     int
	events       []ConflictEvent
	start        int
	count        int
	nextSequence uint64
	notify       chan struct{}
	closed       bool
}

// NewConflictEventLog creates an opt-in bounded conflict-event history. Zero
// selects DefaultConflictEventCapacity; negative and oversized values fail.
func NewConflictEventLog(capacity int) (*ConflictEventLog, error) {
	if capacity == 0 {
		capacity = DefaultConflictEventCapacity
	}
	if capacity < 1 || capacity > MaxConflictEventCapacity {
		return nil, ErrConflictEventCapacityInvalid
	}
	return &ConflictEventLog{
		capacity:     capacity,
		events:       make([]ConflictEvent, capacity),
		nextSequence: 1,
		notify:       make(chan struct{}),
	}, nil
}

// Read returns events after the supplied sequence. A non-positive limit reads
// all retained events. The returned slice is detached from the ring buffer.
func (log *ConflictEventLog) Read(after uint64, limit int) ([]ConflictEvent, error) {
	if log == nil {
		return nil, ErrConflictEventLogClosed
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if err := log.validateCursorLocked(after); err != nil {
		return nil, err
	}
	if log.count == 0 {
		return nil, nil
	}
	first := log.firstSequenceLocked()
	last := log.nextSequence - 1
	if after >= last {
		return nil, nil
	}
	available64 := last - after
	if available64 > uint64(log.count) {
		available64 = uint64(log.count)
	}
	if limit > 0 && available64 > uint64(limit) {
		available64 = uint64(limit)
	}
	available := int(available64)
	result := make([]ConflictEvent, available)
	offset := int(after + 1 - first)
	for index := range result {
		result[index] = log.events[(log.start+offset+index)%log.capacity]
	}
	return result, nil
}

// Wait blocks until at least one event after the supplied sequence exists,
// the log closes, the cursor expires, or ctx is canceled. Call Read afterward.
func (log *ConflictEventLog) Wait(ctx context.Context, after uint64) error {
	if log == nil {
		return ErrConflictEventLogClosed
	}
	if ctx == nil {
		return context.Canceled
	}
	for {
		log.mu.Lock()
		if err := log.validateCursorLocked(after); err != nil {
			log.mu.Unlock()
			return err
		}
		if log.nextSequence-1 > after {
			log.mu.Unlock()
			return nil
		}
		if log.closed {
			log.mu.Unlock()
			return ErrConflictEventLogClosed
		}
		notify := log.notify
		log.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notify:
		}
	}
}

// Close wakes waiters and prevents future events from being retained. Existing
// events remain readable for inspection.
func (log *ConflictEventLog) Close() {
	if log == nil {
		return
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.closed {
		return
	}
	log.closed = true
	close(log.notify)
}

// Stats returns bounded retention and eviction counters without allocating.
func (log *ConflictEventLog) Stats() ConflictEventLogStats {
	if log == nil {
		return ConflictEventLogStats{}
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	stats := ConflictEventLogStats{Capacity: log.capacity, Retained: log.count}
	if log.count == 0 {
		return stats
	}
	stats.FirstSequence = log.firstSequenceLocked()
	stats.LastSequence = log.nextSequence - 1
	stats.Dropped = stats.FirstSequence - 1
	return stats
}

func (log *ConflictEventLog) append(event ConflictEvent) {
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.closed {
		return
	}
	event.Sequence = log.nextSequence
	log.nextSequence++
	if log.count < log.capacity {
		log.events[(log.start+log.count)%log.capacity] = event
		log.count++
	} else {
		log.events[log.start] = event
		log.start = (log.start + 1) % log.capacity
	}
	close(log.notify)
	log.notify = make(chan struct{})
}

func (log *ConflictEventLog) firstSequenceLocked() uint64 {
	return log.nextSequence - uint64(log.count)
}

func (log *ConflictEventLog) validateCursorLocked(after uint64) error {
	if log.count == 0 {
		return nil
	}
	if after < log.firstSequenceLocked()-1 {
		return ErrConflictEventCursorExpired
	}
	return nil
}
