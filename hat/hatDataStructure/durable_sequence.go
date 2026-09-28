package hatDataStructure

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrDurableSequenceNil reports a method call on a nil sequence.
	ErrDurableSequenceNil = errors.New("hatDataStructure: durable sequence is nil")
	// ErrDurableSequencePersistRequired reports a missing durable writer.
	ErrDurableSequencePersistRequired = errors.New("hatDataStructure: durable sequence persistence callback is required")
	// ErrDurableSequencePersist reports a failed durable write.
	ErrDurableSequencePersist = errors.New("hatDataStructure: durable sequence persistence failed")
	// ErrDurableSequenceOverflow reports that no next value is representable.
	ErrDurableSequenceOverflow = errors.New("hatDataStructure: durable sequence overflow")
)

// DurableSequence allocates strictly increasing values behind a caller-owned
// durable write. The current value advances only after Persist accepts the
// candidate, so a failed write does not consume a sequence value.
//
// Persist is called while the sequence lock is held and must atomically publish
// the supplied current value before returning nil. It must not call back into
// the sequence. Loading a previously persisted value is done by passing it to
// NewDurableSequence after restart.
type DurableSequence struct {
	mu      sync.Mutex
	current uint64
	persist func(uint64) error
}

// NewDurableSequence creates a sequence whose current value is current.
// Persist is required so callers cannot accidentally use a non-durable
// allocator under a durable name.
func NewDurableSequence(current uint64, persist func(uint64) error) (*DurableSequence, error) {
	if persist == nil {
		return nil, ErrDurableSequencePersistRequired
	}
	return &DurableSequence{current: current, persist: persist}, nil
}

// Current returns the last value durably accepted by the sequence.
func (sequence *DurableSequence) Current() uint64 {
	if sequence == nil {
		return 0
	}
	sequence.mu.Lock()
	current := sequence.current
	sequence.mu.Unlock()
	return current
}

// Next durably publishes and returns the next sequence value. A persistence
// error leaves Current unchanged, allowing a caller to retry the same value.
func (sequence *DurableSequence) Next() (uint64, error) {
	if sequence == nil {
		return 0, ErrDurableSequenceNil
	}
	sequence.mu.Lock()
	defer sequence.mu.Unlock()
	if sequence.current == ^uint64(0) {
		return 0, ErrDurableSequenceOverflow
	}
	next := sequence.current + 1
	if err := sequence.persist(next); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrDurableSequencePersist, err)
	}
	sequence.current = next
	return next, nil
}
