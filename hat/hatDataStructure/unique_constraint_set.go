package hatDataStructure

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	// ErrUniqueConstraintSetNil indicates an operation on a nil constraint set.
	ErrUniqueConstraintSetNil = errors.New("hatDataStructure: unique constraint set is nil")
	// ErrUniqueConstraintDefinitionInvalid indicates an unusable constraint list.
	ErrUniqueConstraintDefinitionInvalid = errors.New("hatDataStructure: unique constraint definition is invalid")
	// ErrUniqueConstraintDuplicateKey indicates that another row owns a key.
	ErrUniqueConstraintDuplicateKey = errors.New("hatDataStructure: unique constraint key already exists")
)

const (
	// MaxUniqueConstraintSetConstraints bounds the number of alternate keys in
	// one set so malformed schema input cannot create unbounded owner maps.
	MaxUniqueConstraintSetConstraints = 64
	// MaxUniqueConstraintSetCapacity bounds the initial row/key-map hint.
	MaxUniqueConstraintSetCapacity = 1 << 20
)

// UniqueConstraint extracts one alternate unique key from a row. Extractors
// should be deterministic and side-effect free, just like HashIndex extractors.
type UniqueConstraint[T any] struct {
	Name    string
	Extract func(T) string
}

// UniqueConstraintSetOptions controls initial row and key-map capacity.
type UniqueConstraintSetOptions struct {
	Capacity int
}

// UniqueConstraintEntry is an owned row returned by a lookup.
type UniqueConstraintEntry[T any] struct {
	ID    uint64
	Value T
}

// UniqueConstraintConflict identifies the first deterministic constraint that
// rejected an upsert. It unwraps to ErrUniqueConstraintDuplicateKey.
type UniqueConstraintConflict struct {
	ConstraintName string
	Key            string
	OwnerID        uint64
}

func (conflict UniqueConstraintConflict) Error() string {
	return fmt.Sprintf("%s: constraint %q key %q is owned by row %d", ErrUniqueConstraintDuplicateKey, conflict.ConstraintName, conflict.Key, conflict.OwnerID)
}

func (conflict UniqueConstraintConflict) Unwrap() error {
	return ErrUniqueConstraintDuplicateKey
}

type uniqueConstraintEntry[T any] struct {
	value T
}

// UniqueConstraintSet atomically maintains multiple alternate unique keys for
// one row store. It checks every constraint before changing any owner map, so
// a rejected update cannot publish a partial key or remove an old key.
type UniqueConstraintSet[T any] struct {
	mu          sync.RWMutex
	constraints []UniqueConstraint[T]
	byName      map[string]int
	owners      []map[string]uint64
	entries     map[uint64]uniqueConstraintEntry[T]
}

// NewUniqueConstraintSet creates a set with the supplied named constraints.
// Constraint names are trimmed and must be unique.
func NewUniqueConstraintSet[T any](constraints []UniqueConstraint[T], options UniqueConstraintSetOptions) (*UniqueConstraintSet[T], error) {
	if len(constraints) == 0 {
		return nil, fmt.Errorf("%w: at least one constraint is required", ErrUniqueConstraintDefinitionInvalid)
	}
	if len(constraints) > MaxUniqueConstraintSetConstraints {
		return nil, fmt.Errorf("%w: constraint count %d exceeds %d", ErrUniqueConstraintDefinitionInvalid, len(constraints), MaxUniqueConstraintSetConstraints)
	}
	if options.Capacity < 0 {
		options.Capacity = 0
	}
	if options.Capacity > MaxUniqueConstraintSetCapacity {
		return nil, fmt.Errorf("%w: capacity %d exceeds %d", ErrUniqueConstraintDefinitionInvalid, options.Capacity, MaxUniqueConstraintSetCapacity)
	}
	set := &UniqueConstraintSet[T]{
		constraints: make([]UniqueConstraint[T], len(constraints)),
		byName:      make(map[string]int, len(constraints)),
		owners:      make([]map[string]uint64, len(constraints)),
		entries:     make(map[uint64]uniqueConstraintEntry[T], options.Capacity),
	}
	for index, constraint := range constraints {
		constraint.Name = strings.TrimSpace(constraint.Name)
		if constraint.Name == "" || constraint.Extract == nil {
			return nil, fmt.Errorf("%w: constraint %d requires a name and extractor", ErrUniqueConstraintDefinitionInvalid, index)
		}
		if _, exists := set.byName[constraint.Name]; exists {
			return nil, fmt.Errorf("%w: duplicate constraint name %q", ErrUniqueConstraintDefinitionInvalid, constraint.Name)
		}
		set.constraints[index] = constraint
		set.byName[constraint.Name] = index
		set.owners[index] = make(map[string]uint64, options.Capacity)
	}
	return set, nil
}

// Upsert inserts or replaces a row after all alternate keys pass validation.
// The common case of eight or fewer constraints uses stack scratch only.
func (set *UniqueConstraintSet[T]) Upsert(id uint64, value T) error {
	if set == nil {
		return ErrUniqueConstraintSetNil
	}
	var stackKeys [8]string
	keys := stackKeys[:len(set.constraints)]
	if len(set.constraints) > len(stackKeys) {
		keys = make([]string, len(set.constraints))
	}
	for index, constraint := range set.constraints {
		keys[index] = constraint.Extract(value)
	}

	set.mu.Lock()
	defer set.mu.Unlock()
	if len(set.constraints) == 0 || len(set.owners) != len(set.constraints) {
		return ErrUniqueConstraintDefinitionInvalid
	}
	for index, key := range keys {
		if owner, exists := set.owners[index][key]; exists && owner != id {
			return UniqueConstraintConflict{
				ConstraintName: set.constraints[index].Name,
				Key:            key,
				OwnerID:        owner,
			}
		}
	}

	current, exists := set.entries[id]
	if exists {
		for index, constraint := range set.constraints {
			delete(set.owners[index], constraint.Extract(current.value))
		}
	}
	set.entries[id] = uniqueConstraintEntry[T]{value: value}
	for index, key := range keys {
		set.owners[index][key] = id
	}
	return nil
}

// Delete removes a row and all of its alternate key ownership.
func (set *UniqueConstraintSet[T]) Delete(id uint64) bool {
	if set == nil {
		return false
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	entry, exists := set.entries[id]
	if !exists {
		return false
	}
	for index, constraint := range set.constraints {
		delete(set.owners[index], constraint.Extract(entry.value))
	}
	delete(set.entries, id)
	return true
}

// Lookup returns the row owning key under one named constraint.
func (set *UniqueConstraintSet[T]) Lookup(constraintName, key string) (UniqueConstraintEntry[T], bool) {
	if set == nil {
		return UniqueConstraintEntry[T]{}, false
	}
	set.mu.RLock()
	defer set.mu.RUnlock()
	index, exists := set.byName[constraintName]
	if !exists || index < 0 || index >= len(set.owners) {
		return UniqueConstraintEntry[T]{}, false
	}
	id, exists := set.owners[index][key]
	if !exists {
		return UniqueConstraintEntry[T]{}, false
	}
	entry, exists := set.entries[id]
	if !exists {
		return UniqueConstraintEntry[T]{}, false
	}
	return UniqueConstraintEntry[T]{ID: id, Value: entry.value}, true
}

// LookupByID returns a row by its stable ID.
func (set *UniqueConstraintSet[T]) LookupByID(id uint64) (UniqueConstraintEntry[T], bool) {
	if set == nil {
		return UniqueConstraintEntry[T]{}, false
	}
	set.mu.RLock()
	defer set.mu.RUnlock()
	entry, exists := set.entries[id]
	if !exists {
		return UniqueConstraintEntry[T]{}, false
	}
	return UniqueConstraintEntry[T]{ID: id, Value: entry.value}, true
}

// Contains reports whether key is owned under a named constraint.
func (set *UniqueConstraintSet[T]) Contains(constraintName, key string) bool {
	if set == nil {
		return false
	}
	set.mu.RLock()
	defer set.mu.RUnlock()
	index, exists := set.byName[constraintName]
	if !exists || index < 0 || index >= len(set.owners) {
		return false
	}
	_, exists = set.owners[index][key]
	return exists
}

// Len returns the number of rows in the set.
func (set *UniqueConstraintSet[T]) Len() int {
	if set == nil {
		return 0
	}
	set.mu.RLock()
	defer set.mu.RUnlock()
	return len(set.entries)
}

// ConstraintNames returns the declaration-order names owned by the set.
func (set *UniqueConstraintSet[T]) ConstraintNames() []string {
	if set == nil {
		return nil
	}
	set.mu.RLock()
	defer set.mu.RUnlock()
	names := make([]string, len(set.constraints))
	for index, constraint := range set.constraints {
		names[index] = constraint.Name
	}
	return names
}

// Clear removes all rows while retaining the constraint definitions and map
// capacity for reuse.
func (set *UniqueConstraintSet[T]) Clear() {
	if set == nil {
		return
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	for index := range set.owners {
		for key := range set.owners[index] {
			delete(set.owners[index], key)
		}
	}
	for id := range set.entries {
		delete(set.entries, id)
	}
}
