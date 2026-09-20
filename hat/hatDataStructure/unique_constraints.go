package hatDataStructure

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	ErrUniqueConstraintSetNil          = errors.New("unique constraint set is nil")
	ErrUniqueConstraintDefinitionInvalid = errors.New("unique constraint definition is invalid")
	ErrUniqueConstraintDuplicateID     = errors.New("unique constraint set id already exists")
	ErrUniqueConstraintViolation       = errors.New("unique constraint violated")
)

// UniqueConstraint describes one unique key extractor. A false extractor
// result means that the row has no value for this constraint and does not
// participate in uniqueness checks, matching SQL's nullable unique-key rule.
type UniqueConstraint[T any, K comparable] struct {
	Name string
	Key  func(T) (K, bool)
}

// UniqueConstraintViolationError identifies the named constraint that rejected
// an insert or upsert. It unwraps to ErrUniqueConstraintViolation.
type UniqueConstraintViolationError struct {
	Constraint string
}

func (err *UniqueConstraintViolationError) Error() string {
	if err == nil {
		return ErrUniqueConstraintViolation.Error()
	}
	return fmt.Sprintf("%s: %s", ErrUniqueConstraintViolation, err.Constraint)
}

func (err *UniqueConstraintViolationError) Unwrap() error {
	return ErrUniqueConstraintViolation
}

type uniqueConstraintDefinition[T any, K comparable] struct {
	name string
	key  func(T) (K, bool)
}

type uniqueConstraintKey[K comparable] struct {
	value   K
	present bool
}

// UniqueConstraintSet atomically maintains several unique indexes over the
// same ID-keyed value set. A rejected Upsert leaves every value and index
// unchanged. The set is safe for concurrent access.
type UniqueConstraintSet[T any, K comparable] struct {
	mu          sync.RWMutex
	constraints []uniqueConstraintDefinition[T, K]
	owners      []map[K]uint64
	keys        map[uint64][]uniqueConstraintKey[K]
}

// NewUniqueConstraintSet creates a set with one or more named constraints.
// Constraint names must be non-empty and unique, and every key extractor is
// required.
func NewUniqueConstraintSet[T any, K comparable](constraints []UniqueConstraint[T, K]) (*UniqueConstraintSet[T, K], error) {
	if len(constraints) == 0 {
		return nil, ErrUniqueConstraintDefinitionInvalid
	}
	definitions := make([]uniqueConstraintDefinition[T, K], len(constraints))
	owners := make([]map[K]uint64, len(constraints))
	seen := make(map[string]struct{}, len(constraints))
	for index, constraint := range constraints {
		name := strings.TrimSpace(constraint.Name)
		if name == "" || constraint.Key == nil {
			return nil, fmt.Errorf("%w: constraint %d requires a name and key extractor", ErrUniqueConstraintDefinitionInvalid, index)
		}
		if _, exists := seen[name]; exists {
			return nil, fmt.Errorf("%w: duplicate constraint %q", ErrUniqueConstraintDefinitionInvalid, name)
		}
		seen[name] = struct{}{}
		definitions[index] = uniqueConstraintDefinition[T, K]{name: name, key: constraint.Key}
		owners[index] = make(map[K]uint64)
	}
	return &UniqueConstraintSet[T, K]{
		constraints: definitions,
		owners:      owners,
		keys:        make(map[uint64][]uniqueConstraintKey[K]),
	}, nil
}

// Insert adds a value under id and checks every constraint before changing
// any value or index.
func (set *UniqueConstraintSet[T, K]) Insert(id uint64, value T) error {
	if set == nil {
		return ErrUniqueConstraintSetNil
	}
	keys := set.deriveKeys(value)
	set.mu.Lock()
	defer set.mu.Unlock()
	if _, exists := set.keys[id]; exists {
		return ErrUniqueConstraintDuplicateID
	}
	if conflict := set.conflictLocked(id, keys); conflict >= 0 {
		return &UniqueConstraintViolationError{Constraint: set.constraints[conflict].name}
	}
	set.applyLocked(id, keys)
	return nil
}

// Upsert inserts or replaces a value. All unique keys are checked before the
// old value or any index is changed.
func (set *UniqueConstraintSet[T, K]) Upsert(id uint64, value T) error {
	if set == nil {
		return ErrUniqueConstraintSetNil
	}
	keys := set.deriveKeys(value)
	set.mu.Lock()
	defer set.mu.Unlock()
	if conflict := set.conflictLocked(id, keys); conflict >= 0 {
		return &UniqueConstraintViolationError{Constraint: set.constraints[conflict].name}
	}
	set.applyLocked(id, keys)
	return nil
}

// Contains reports whether id currently participates in the constraints.
func (set *UniqueConstraintSet[T, K]) Contains(id uint64) bool {
	if set == nil {
		return false
	}
	set.mu.RLock()
	defer set.mu.RUnlock()
	_, ok := set.keys[id]
	return ok
}

// Delete removes id and all of its unique keys.
func (set *UniqueConstraintSet[T, K]) Delete(id uint64) bool {
	if set == nil {
		return false
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	if _, exists := set.keys[id]; !exists {
		return false
	}
	set.removeLocked(id)
	return true
}

// Len returns the number of values in the set.
func (set *UniqueConstraintSet[T, K]) Len() int {
	if set == nil {
		return 0
	}
	set.mu.RLock()
	defer set.mu.RUnlock()
	return len(set.keys)
}

// ConstraintCount returns the number of maintained unique indexes.
func (set *UniqueConstraintSet[T, K]) ConstraintCount() int {
	if set == nil {
		return 0
	}
	set.mu.RLock()
	defer set.mu.RUnlock()
	return len(set.constraints)
}

// Reset removes all values while retaining the constraint definitions and map
// capacity for reuse.
func (set *UniqueConstraintSet[T, K]) Reset() {
	if set == nil {
		return
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	for id := range set.keys {
		delete(set.keys, id)
	}
	for _, owners := range set.owners {
		for key := range owners {
			delete(owners, key)
		}
	}
}

func (set *UniqueConstraintSet[T, K]) deriveKeys(value T) []uniqueConstraintKey[K] {
	keys := make([]uniqueConstraintKey[K], len(set.constraints))
	for index, constraint := range set.constraints {
		keys[index].value, keys[index].present = constraint.key(value)
	}
	return keys
}

func (set *UniqueConstraintSet[T, K]) conflictLocked(id uint64, keys []uniqueConstraintKey[K]) int {
	for index, key := range keys {
		if !key.present {
			continue
		}
		if owner, exists := set.owners[index][key.value]; exists && owner != id {
			return index
		}
	}
	return -1
}

func (set *UniqueConstraintSet[T, K]) applyLocked(id uint64, keys []uniqueConstraintKey[K]) {
	if _, exists := set.keys[id]; exists {
		set.removeKeysLocked(id, set.keys[id])
	}
	for index, key := range keys {
		if key.present {
			set.owners[index][key.value] = id
		}
	}
	set.keys[id] = keys
}

func (set *UniqueConstraintSet[T, K]) removeLocked(id uint64) {
	set.removeKeysLocked(id, set.keys[id])
	delete(set.keys, id)
}

func (set *UniqueConstraintSet[T, K]) removeKeysLocked(id uint64, keys []uniqueConstraintKey[K]) {
	for index, key := range keys {
		if !key.present {
			continue
		}
		if owner, exists := set.owners[index][key.value]; exists && owner == id {
			delete(set.owners[index], key.value)
		}
	}
}
