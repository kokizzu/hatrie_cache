package hatPipeline

import (
	"errors"
	"sync"
)

var (
	// ErrAntichainNil reports a method call on a nil antichain.
	ErrAntichainNil = errors.New("hatPipeline: antichain is nil")
	// ErrAntichainComparatorRequired reports a missing partial-order relation.
	ErrAntichainComparatorRequired = errors.New("hatPipeline: antichain comparator is required")
)

// Antichain stores the minimal elements of a partially ordered timestamp set.
// A newly inserted timestamp is discarded when an existing element is less
// than or equal to it. Existing elements dominated by the new timestamp are
// removed. The comparator must be a stable reflexive partial-order relation.
// Methods are safe for concurrent callers; the comparator runs while the
// antichain lock is held and should not call back into the same antichain.
type Antichain[T any] struct {
	mu        sync.RWMutex
	lessEqual func(T, T) bool
	elements  []T
}

// NewAntichain returns an empty antichain using lessEqual as its partial order.
func NewAntichain[T any](lessEqual func(T, T) bool) (*Antichain[T], error) {
	if lessEqual == nil {
		return nil, ErrAntichainComparatorRequired
	}
	return &Antichain[T]{lessEqual: lessEqual}, nil
}

// Insert adds timestamp if it is not dominated and removes elements that it
// dominates. The bool is true only when the minimal frontier changes.
func (antichain *Antichain[T]) Insert(timestamp T) (bool, error) {
	if antichain == nil {
		return false, ErrAntichainNil
	}
	antichain.mu.Lock()
	defer antichain.mu.Unlock()
	for _, existing := range antichain.elements {
		if antichain.lessEqual(existing, timestamp) {
			return false, nil
		}
	}
	oldLength := len(antichain.elements)
	write := 0
	for _, existing := range antichain.elements {
		if antichain.lessEqual(timestamp, existing) {
			continue
		}
		antichain.elements[write] = existing
		write++
	}
	var zero T
	for index := write; index < oldLength; index++ {
		antichain.elements[index] = zero
	}
	antichain.elements = append(antichain.elements[:write], timestamp)
	return true, nil
}

// Covers reports whether any minimal element is less than or equal to
// timestamp. It is the usual frontier query: the timestamp is available once
// one antichain element has advanced no later than it.
func (antichain *Antichain[T]) Covers(timestamp T) (bool, error) {
	if antichain == nil {
		return false, ErrAntichainNil
	}
	antichain.mu.RLock()
	defer antichain.mu.RUnlock()
	for _, existing := range antichain.elements {
		if antichain.lessEqual(existing, timestamp) {
			return true, nil
		}
	}
	return false, nil
}

// Len returns the number of incomparable minimal elements. A nil antichain
// has length zero.
func (antichain *Antichain[T]) Len() int {
	if antichain == nil {
		return 0
	}
	antichain.mu.RLock()
	defer antichain.mu.RUnlock()
	return len(antichain.elements)
}

// Snapshot returns a copy of the current minimal elements. The order is the
// insertion-survivor order and is not a timestamp sort order.
func (antichain *Antichain[T]) Snapshot() ([]T, error) {
	if antichain == nil {
		return nil, ErrAntichainNil
	}
	antichain.mu.RLock()
	defer antichain.mu.RUnlock()
	return append([]T(nil), antichain.elements...), nil
}

// Clear removes all minimal elements while retaining the allocated backing
// storage for reuse.
func (antichain *Antichain[T]) Clear() {
	if antichain == nil {
		return
	}
	antichain.mu.Lock()
	defer antichain.mu.Unlock()
	var zero T
	for index := range antichain.elements {
		antichain.elements[index] = zero
	}
	antichain.elements = antichain.elements[:0]
}
