package hatDataStructure

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	// ErrCrossIndexUniqueNil indicates that an operation was attempted on a
	// nil cross-index uniqueness registry.
	ErrCrossIndexUniqueNil = errors.New("hatDataStructure: cross-index unique registry is nil")
	// ErrCrossIndexUniqueInvalid indicates an invalid registry definition or
	// key extraction result.
	ErrCrossIndexUniqueInvalid = errors.New("hatDataStructure: cross-index unique definition is invalid")
	// ErrCrossIndexUniqueDuplicate indicates that a key is already owned by a
	// different ID in one of the constraints.
	ErrCrossIndexUniqueDuplicate = errors.New("hatDataStructure: cross-index unique key already exists")
	// ErrCrossIndexUniqueConstraint indicates that a named constraint does not
	// exist in the registry.
	ErrCrossIndexUniqueConstraint = errors.New("hatDataStructure: cross-index unique constraint is not defined")
)

// CrossIndexUniqueConstraint defines one named uniqueness key. Key must
// return a canonical representation; separate constraints may use different
// encodings while one Upsert remains atomic across all of them. Key is called
// while the registry write lock is held, so it should be deterministic and
// non-blocking.
type CrossIndexUniqueConstraint[T any] struct {
	Name string
	Key  func(T) (string, error)
}

type crossIndexUniqueEntry[T any] struct {
	value T
	keys  []string
}

// CrossIndexUnique atomically maintains several named unique indexes for the
// same values. It is useful when a row must satisfy both single-field and
// composite uniqueness rules without leaving one index updated after another
// rule rejects an upsert. The constraint definition is immutable after
// construction and all operations are safe for concurrent use.
type CrossIndexUnique[T any] struct {
	mu           sync.RWMutex
	constraints  []CrossIndexUniqueConstraint[T]
	byName       map[string]int
	byConstraint []map[string]uint64
	entries      map[uint64]crossIndexUniqueEntry[T]
}

const crossIndexUniqueInlineConstraintLimit = 8

// NewCrossIndexUnique creates a registry with the supplied named constraints.
// Names must be non-empty and unique, and every constraint must have a key
// function. Capacity is an initial map sizing hint and may be zero.
func NewCrossIndexUnique[T any](constraints []CrossIndexUniqueConstraint[T], capacity int) (*CrossIndexUnique[T], error) {
	if len(constraints) == 0 {
		return nil, fmt.Errorf("%w: at least one constraint is required", ErrCrossIndexUniqueInvalid)
	}
	if capacity < 0 {
		capacity = 0
	}
	registry := &CrossIndexUnique[T]{
		constraints:  make([]CrossIndexUniqueConstraint[T], len(constraints)),
		byName:       make(map[string]int, len(constraints)),
		byConstraint: make([]map[string]uint64, len(constraints)),
		entries:      make(map[uint64]crossIndexUniqueEntry[T], capacity),
	}
	copy(registry.constraints, constraints)
	for index, constraint := range registry.constraints {
		constraint.Name = strings.TrimSpace(constraint.Name)
		if constraint.Name == "" || constraint.Key == nil {
			return nil, fmt.Errorf("%w: constraint %d requires a name and key function", ErrCrossIndexUniqueInvalid, index)
		}
		if _, exists := registry.byName[constraint.Name]; exists {
			return nil, fmt.Errorf("%w: duplicate constraint %q", ErrCrossIndexUniqueInvalid, constraint.Name)
		}
		registry.constraints[index] = constraint
		registry.byName[constraint.Name] = index
		registry.byConstraint[index] = make(map[string]uint64, capacity)
	}
	return registry, nil
}

// Upsert validates every constraint before changing any state. If one key is
// already owned by another ID, no constraint or value is modified.
func (registry *CrossIndexUnique[T]) Upsert(id uint64, value T) error {
	if registry == nil {
		return ErrCrossIndexUniqueNil
	}
	if len(registry.constraints) == 0 {
		return fmt.Errorf("%w: no constraints configured", ErrCrossIndexUniqueInvalid)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	current, exists := registry.entries[id]
	if exists && len(current.keys) != len(registry.constraints) {
		return fmt.Errorf("%w: stored key count does not match constraint count", ErrCrossIndexUniqueInvalid)
	}
	var inlineKeys [crossIndexUniqueInlineConstraintLimit]string
	var keys []string
	if len(registry.constraints) <= len(inlineKeys) {
		keys = inlineKeys[:len(registry.constraints)]
	} else {
		keys = make([]string, len(registry.constraints))
	}
	for index, constraint := range registry.constraints {
		key, err := constraint.Key(value)
		if err != nil {
			return fmt.Errorf("%w: constraint %q: %w", ErrCrossIndexUniqueInvalid, constraint.Name, err)
		}
		keys[index] = key
		if owner, exists := registry.byConstraint[index][key]; exists && owner != id {
			return fmt.Errorf("%w: constraint %q", ErrCrossIndexUniqueDuplicate, constraint.Name)
		}
	}
	if exists {
		changed := false
		for index, key := range keys {
			if current.keys[index] != key {
				changed = true
				break
			}
		}
		if !changed {
			current.value = value
			registry.entries[id] = current
			return nil
		}
		for index, oldKey := range current.keys {
			if oldKey != keys[index] {
				delete(registry.byConstraint[index], oldKey)
			}
		}
		for index, key := range keys {
			registry.byConstraint[index][key] = id
			current.keys[index] = key
		}
		current.value = value
		registry.entries[id] = current
		return nil
	}
	storedKeys := make([]string, len(keys))
	copy(storedKeys, keys)
	for index, key := range keys {
		registry.byConstraint[index][key] = id
	}
	registry.entries[id] = crossIndexUniqueEntry[T]{value: value, keys: storedKeys}
	return nil
}

// Delete removes an ID from the value store and every named constraint. It
// reports whether the ID was present.
func (registry *CrossIndexUnique[T]) Delete(id uint64) bool {
	if registry == nil {
		return false
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	entry, exists := registry.entries[id]
	if !exists {
		return false
	}
	for index, key := range entry.keys {
		delete(registry.byConstraint[index], key)
	}
	delete(registry.entries, id)
	return true
}

// Lookup returns the value owned by key in a named constraint.
func (registry *CrossIndexUnique[T]) Lookup(constraintName, key string) (T, bool, error) {
	var zero T
	if registry == nil {
		return zero, false, ErrCrossIndexUniqueNil
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	index, ok := registry.byName[constraintName]
	if !ok {
		return zero, false, fmt.Errorf("%w: %q", ErrCrossIndexUniqueConstraint, constraintName)
	}
	id, ok := registry.byConstraint[index][key]
	if !ok {
		return zero, false, nil
	}
	entry, ok := registry.entries[id]
	if !ok {
		return zero, false, fmt.Errorf("%w: missing owner for constraint %q", ErrCrossIndexUniqueInvalid, constraintName)
	}
	return entry.value, true, nil
}

// Contains reports whether key is owned by a named constraint.
func (registry *CrossIndexUnique[T]) Contains(constraintName, key string) (bool, error) {
	if registry == nil {
		return false, ErrCrossIndexUniqueNil
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	index, ok := registry.byName[constraintName]
	if !ok {
		return false, fmt.Errorf("%w: %q", ErrCrossIndexUniqueConstraint, constraintName)
	}
	_, ok = registry.byConstraint[index][key]
	return ok, nil
}

// ConstraintNames returns the immutable constraint names in definition order.
func (registry *CrossIndexUnique[T]) ConstraintNames() []string {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	names := make([]string, len(registry.constraints))
	for index, constraint := range registry.constraints {
		names[index] = constraint.Name
	}
	return names
}

// Len returns the number of values that satisfy all constraints.
func (registry *CrossIndexUnique[T]) Len() int {
	if registry == nil {
		return 0
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	return len(registry.entries)
}

// Clear removes all values while retaining the constraint definitions and map
// capacity for reuse.
func (registry *CrossIndexUnique[T]) Clear() {
	if registry == nil {
		return
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	for index := range registry.byConstraint {
		for key := range registry.byConstraint[index] {
			delete(registry.byConstraint[index], key)
		}
	}
	for id := range registry.entries {
		delete(registry.entries, id)
	}
}
