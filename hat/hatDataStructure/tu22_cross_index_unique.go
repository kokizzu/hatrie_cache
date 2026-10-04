package hatDataStructure

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

const (
	// MaxCrossIndexUniqueConstraints bounds the number of projections owned by
	// one set so a malformed schema cannot create unbounded per-row metadata.
	MaxCrossIndexUniqueConstraints = 64
	// MaxCrossIndexUniqueConstraintNameBytes bounds names retained in errors and
	// diagnostics.
	MaxCrossIndexUniqueConstraintNameBytes = 128
	// MaxCrossIndexUniqueKeyBytes bounds one retained canonical key.
	MaxCrossIndexUniqueKeyBytes = 1 << 20
	// MaxCrossIndexUniqueBatch bounds one atomic mutation group.
	MaxCrossIndexUniqueBatch = 4096
)

var (
	// ErrCrossIndexUniqueSetNil indicates an operation on a nil set.
	ErrCrossIndexUniqueSetNil = errors.New("hatDataStructure: cross-index unique set is nil")
	// ErrCrossIndexUniqueConstraintInvalid indicates an invalid definition.
	ErrCrossIndexUniqueConstraintInvalid = errors.New("hatDataStructure: cross-index unique constraint is invalid")
	// ErrCrossIndexUniqueConflict indicates that a candidate key belongs to a
	// different row. CrossIndexUniqueConflictError provides the details.
	ErrCrossIndexUniqueConflict = errors.New("hatDataStructure: cross-index unique constraint conflict")
	// ErrCrossIndexUniqueKeyInvalid indicates that a present key is too large.
	ErrCrossIndexUniqueKeyInvalid = errors.New("hatDataStructure: cross-index unique key is invalid")
	// ErrCrossIndexUniqueBatchInvalid indicates a malformed atomic batch.
	ErrCrossIndexUniqueBatchInvalid = errors.New("hatDataStructure: cross-index unique batch is invalid")
)

// CrossIndexUniqueConstraint derives one canonical key from a row. Key should
// return present=false for SQL NULL-like values so multiple absent values do
// not conflict. The extractor is evaluated before a mutation is published.
type CrossIndexUniqueConstraint[T any] struct {
	Name string
	Key  func(T) (key string, present bool, err error)
}

// CrossIndexUniqueMutation is one operation in an atomic mutation batch.
// Delete ignores Value and removes the row if it exists.
type CrossIndexUniqueMutation[T any] struct {
	ID     uint64
	Value  T
	Delete bool
}

// CrossIndexUniqueConflictError identifies the constraint and row that already
// owns a conflicting key. The key itself is intentionally not retained in the
// error so callers can use sensitive identifiers without leaking them.
type CrossIndexUniqueConflictError struct {
	Constraint  string
	OwnerID     uint64
	CandidateID uint64
}

func (err *CrossIndexUniqueConflictError) Error() string {
	if err == nil {
		return ErrCrossIndexUniqueConflict.Error()
	}
	return fmt.Sprintf("%s: constraint=%q owner_id=%d candidate_id=%d", ErrCrossIndexUniqueConflict, err.Constraint, err.OwnerID, err.CandidateID)
}

func (err *CrossIndexUniqueConflictError) Unwrap() error {
	return ErrCrossIndexUniqueConflict
}

type crossIndexUniqueConstraint[T any] struct {
	name string
	key  func(T) (string, bool, error)
}

type crossIndexUniqueKey struct {
	value   string
	present bool
}

type crossIndexUniqueRow[T any] struct {
	value T
	keys  []crossIndexUniqueKey
}

type crossIndexUniqueStagedMutation[T any] struct {
	mutation CrossIndexUniqueMutation[T]
	keys     []crossIndexUniqueKey
}

// CrossIndexUniqueSet atomically owns several named unique projections over
// the same row IDs. It is useful when one space has independent unique email,
// external-ID, or functional keys that must move together on replacement.
// The set is opt-in and safe for concurrent readers and writers.
type CrossIndexUniqueSet[T any] struct {
	mu          sync.RWMutex
	constraints []crossIndexUniqueConstraint[T]
	positions   map[string]int
	owners      []map[string]uint64
	rows        map[uint64]crossIndexUniqueRow[T]
}

// NewCrossIndexUniqueSet validates and creates a set. Capacity is an initial
// sizing hint and may be zero.
func NewCrossIndexUniqueSet[T any](constraints []CrossIndexUniqueConstraint[T], capacity int) (*CrossIndexUniqueSet[T], error) {
	if len(constraints) == 0 || len(constraints) > MaxCrossIndexUniqueConstraints {
		return nil, fmt.Errorf("%w: expected 1..%d constraints", ErrCrossIndexUniqueConstraintInvalid, MaxCrossIndexUniqueConstraints)
	}
	if capacity < 0 {
		capacity = 0
	}
	set := &CrossIndexUniqueSet[T]{
		constraints: make([]crossIndexUniqueConstraint[T], len(constraints)),
		positions:   make(map[string]int, len(constraints)),
		owners:      make([]map[string]uint64, len(constraints)),
		rows:        make(map[uint64]crossIndexUniqueRow[T], capacity),
	}
	for index, constraint := range constraints {
		name := strings.TrimSpace(constraint.Name)
		if name == "" || len(name) > MaxCrossIndexUniqueConstraintNameBytes || constraint.Key == nil {
			return nil, fmt.Errorf("%w: constraint %d requires a bounded name and key extractor", ErrCrossIndexUniqueConstraintInvalid, index)
		}
		if _, exists := set.positions[name]; exists {
			return nil, fmt.Errorf("%w: duplicate constraint %q", ErrCrossIndexUniqueConstraintInvalid, name)
		}
		set.positions[name] = index
		set.constraints[index] = crossIndexUniqueConstraint[T]{name: name, key: constraint.Key}
		set.owners[index] = make(map[string]uint64, capacity)
	}
	return set, nil
}

// Upsert validates all projections before replacing one row. A conflict leaves
// every row and owner map unchanged.
func (set *CrossIndexUniqueSet[T]) Upsert(id uint64, value T) error {
	if set == nil {
		return ErrCrossIndexUniqueSetNil
	}
	var scratch [MaxCrossIndexUniqueConstraints]crossIndexUniqueKey
	keys, err := set.extractKeysInto(scratch[:len(set.constraints)], value)
	if err != nil {
		return err
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	if err := set.validateReadyLocked(); err != nil {
		return err
	}
	if err := set.checkKeysLocked(id, keys, nil); err != nil {
		return err
	}
	current, exists := set.rows[id]
	set.removeRowLocked(id)
	if exists {
		set.installRowLocked(id, value, keys, current.keys)
	} else {
		set.installRowLocked(id, value, keys, nil)
	}
	return nil
}

// ApplyBatch validates every operation and every unique projection before
// publishing the group. Duplicate IDs in one batch are rejected to keep the
// result deterministic and rollback-free.
func (set *CrossIndexUniqueSet[T]) ApplyBatch(mutations []CrossIndexUniqueMutation[T]) error {
	if set == nil {
		return ErrCrossIndexUniqueSetNil
	}
	if len(mutations) > MaxCrossIndexUniqueBatch {
		return fmt.Errorf("%w: maximum mutations %d exceeded", ErrCrossIndexUniqueBatchInvalid, MaxCrossIndexUniqueBatch)
	}
	if len(mutations) == 0 {
		return nil
	}
	staged := make([]crossIndexUniqueStagedMutation[T], len(mutations))
	seenIDs := make(map[uint64]struct{}, len(mutations))
	for index, mutation := range mutations {
		if _, exists := seenIDs[mutation.ID]; exists {
			return fmt.Errorf("%w: duplicate row id %d", ErrCrossIndexUniqueBatchInvalid, mutation.ID)
		}
		seenIDs[mutation.ID] = struct{}{}
		staged[index].mutation = mutation
		if !mutation.Delete {
			keys, err := set.extractKeys(mutation.Value)
			if err != nil {
				return err
			}
			staged[index].keys = keys
		}
	}

	set.mu.Lock()
	defer set.mu.Unlock()
	if err := set.validateReadyLocked(); err != nil {
		return err
	}
	for _, item := range staged {
		if item.mutation.Delete {
			continue
		}
		if err := set.checkKeysLocked(item.mutation.ID, item.keys, seenIDs); err != nil {
			return err
		}
	}
	batchOwners := make([]map[string]uint64, len(set.constraints))
	for index := range set.constraints {
		batchOwners[index] = make(map[string]uint64)
	}
	for _, item := range staged {
		if item.mutation.Delete {
			continue
		}
		for index, key := range item.keys {
			if !key.present {
				continue
			}
			if owner, exists := batchOwners[index][key.value]; exists && owner != item.mutation.ID {
				return &CrossIndexUniqueConflictError{
					Constraint:  set.constraints[index].name,
					OwnerID:     owner,
					CandidateID: item.mutation.ID,
				}
			}
			batchOwners[index][key.value] = item.mutation.ID
		}
	}
	for _, item := range staged {
		set.removeRowLocked(item.mutation.ID)
	}
	for _, item := range staged {
		if !item.mutation.Delete {
			set.installOwnedRowLocked(item.mutation.ID, item.mutation.Value, item.keys)
		}
	}
	return nil
}

// Get returns a copy of one row.
func (set *CrossIndexUniqueSet[T]) Get(id uint64) (T, bool) {
	var zero T
	if set == nil {
		return zero, false
	}
	set.mu.RLock()
	defer set.mu.RUnlock()
	row, ok := set.rows[id]
	if !ok {
		return zero, false
	}
	return row.value, true
}

// LookupID returns the row ID that owns key under constraintName.
func (set *CrossIndexUniqueSet[T]) LookupID(constraintName, key string) (uint64, bool) {
	if set == nil {
		return 0, false
	}
	set.mu.RLock()
	defer set.mu.RUnlock()
	index, ok := set.positions[strings.TrimSpace(constraintName)]
	if !ok || index < 0 || index >= len(set.owners) || set.owners[index] == nil {
		return 0, false
	}
	id, ok := set.owners[index][key]
	return id, ok
}

// Delete removes a row and all its unique projections.
func (set *CrossIndexUniqueSet[T]) Delete(id uint64) bool {
	if set == nil {
		return false
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	if _, ok := set.rows[id]; !ok {
		return false
	}
	set.removeRowLocked(id)
	return true
}

// Len returns the number of present rows.
func (set *CrossIndexUniqueSet[T]) Len() int {
	if set == nil {
		return 0
	}
	set.mu.RLock()
	defer set.mu.RUnlock()
	return len(set.rows)
}

// ConstraintNames returns a stable copy of the configured projection names.
func (set *CrossIndexUniqueSet[T]) ConstraintNames() []string {
	if set == nil {
		return nil
	}
	set.mu.RLock()
	defer set.mu.RUnlock()
	names := make([]string, len(set.constraints))
	for index, constraint := range set.constraints {
		names[index] = constraint.name
	}
	return names
}

// Clear removes rows and retains the validated constraint definitions.
func (set *CrossIndexUniqueSet[T]) Clear() {
	if set == nil {
		return
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	set.rows = nil
	set.rows = make(map[uint64]crossIndexUniqueRow[T])
	set.owners = make([]map[string]uint64, len(set.constraints))
	for index := range set.constraints {
		set.owners[index] = make(map[string]uint64)
	}
}

func (set *CrossIndexUniqueSet[T]) extractKeys(value T) ([]crossIndexUniqueKey, error) {
	return set.extractKeysInto(nil, value)
}

func (set *CrossIndexUniqueSet[T]) validateReadyLocked() error {
	if len(set.constraints) == 0 || len(set.constraints) != len(set.owners) || set.rows == nil {
		return ErrCrossIndexUniqueConstraintInvalid
	}
	for _, owners := range set.owners {
		if owners == nil {
			return ErrCrossIndexUniqueConstraintInvalid
		}
	}
	return nil
}

func (set *CrossIndexUniqueSet[T]) extractKeysInto(dst []crossIndexUniqueKey, value T) ([]crossIndexUniqueKey, error) {
	if cap(dst) < len(set.constraints) {
		dst = make([]crossIndexUniqueKey, len(set.constraints))
	} else {
		dst = dst[:len(set.constraints)]
	}
	for index, constraint := range set.constraints {
		key, present, err := constraint.key(value)
		if err != nil {
			return nil, fmt.Errorf("%w: constraint %q: %v", ErrCrossIndexUniqueConstraintInvalid, constraint.name, err)
		}
		if present && len(key) > MaxCrossIndexUniqueKeyBytes {
			return nil, fmt.Errorf("%w: constraint %q key exceeds %d bytes", ErrCrossIndexUniqueKeyInvalid, constraint.name, MaxCrossIndexUniqueKeyBytes)
		}
		dst[index] = crossIndexUniqueKey{value: key, present: present}
	}
	return dst, nil
}

func (set *CrossIndexUniqueSet[T]) checkKeysLocked(id uint64, keys []crossIndexUniqueKey, replacedIDs map[uint64]struct{}) error {
	for index, key := range keys {
		if !key.present {
			continue
		}
		owner, exists := set.owners[index][key.value]
		if !exists || owner == id {
			continue
		}
		if replacedIDs != nil {
			if _, replaced := replacedIDs[owner]; replaced {
				continue
			}
		}
		return &CrossIndexUniqueConflictError{
			Constraint:  set.constraints[index].name,
			OwnerID:     owner,
			CandidateID: id,
		}
	}
	return nil
}

func (set *CrossIndexUniqueSet[T]) removeRowLocked(id uint64) {
	row, exists := set.rows[id]
	if !exists {
		return
	}
	for index, key := range row.keys {
		if key.present {
			if owner, ok := set.owners[index][key.value]; ok && owner == id {
				delete(set.owners[index], key.value)
			}
		}
	}
	delete(set.rows, id)
}

func (set *CrossIndexUniqueSet[T]) installRowLocked(id uint64, value T, keys, reuse []crossIndexUniqueKey) {
	ownedKeys := reuse
	if cap(ownedKeys) < len(keys) || ownedKeys == nil {
		ownedKeys = make([]crossIndexUniqueKey, len(keys))
	} else {
		ownedKeys = ownedKeys[:len(keys)]
	}
	copy(ownedKeys, keys)
	set.installOwnedRowLocked(id, value, ownedKeys)
}

func (set *CrossIndexUniqueSet[T]) installOwnedRowLocked(id uint64, value T, ownedKeys []crossIndexUniqueKey) {
	for index, key := range ownedKeys {
		if key.present {
			set.owners[index][key.value] = id
		}
	}
	set.rows[id] = crossIndexUniqueRow[T]{value: value, keys: ownedKeys}
}
