package hatDataStructure

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	ErrUniqueIndexGroupNil               = errors.New("hatDataStructure: unique index group is nil")
	ErrUniqueIndexGroupInvalid           = errors.New("hatDataStructure: unique index group is invalid")
	ErrUniqueIndexGroupDuplicate         = errors.New("hatDataStructure: unique index key already exists")
	ErrUniqueIndexGroupConstraintUnknown = errors.New("hatDataStructure: unique index constraint is unknown")
)

const MaxUniqueIndexGroupConstraints = 64

// UniqueIndexConstraint derives one optional unique key from a value. A false
// present result leaves the value unindexed, matching SQL NULL uniqueness
// semantics without retaining the absent key.
type UniqueIndexConstraint[T any] struct {
	Name    string
	Extract func(T) (key string, present bool)
}

// UniqueIndexViolation identifies the constraint that rejected an atomic
// multi-index update. The conflicting key is intentionally not included in the
// error so callers do not accidentally disclose sensitive indexed values.
type UniqueIndexViolation struct {
	Constraint string
	Owner      uint64
}

func (violation UniqueIndexViolation) Error() string {
	return fmt.Sprintf("unique index constraint %q is owned by row %d", violation.Constraint, violation.Owner)
}

func (violation UniqueIndexViolation) Unwrap() error {
	return ErrUniqueIndexGroupDuplicate
}

type uniqueIndexGroupKey struct {
	value   string
	present bool
}

// UniqueIndexGroup atomically maintains multiple unique secondary indexes for
// one caller-owned row set. All constraints are checked before any old key is
// removed, so a failed replacement preserves every index exactly.
type UniqueIndexGroup[T any] struct {
	mu          sync.RWMutex
	constraints []UniqueIndexConstraint[T]
	byName      map[string]int
	owners      []map[string]uint64
	entries     map[uint64][]uniqueIndexGroupKey
}

// NewUniqueIndexGroup creates a bounded group of unique indexes. A negative
// capacity is treated as zero; it is only an initial map sizing hint.
func NewUniqueIndexGroup[T any](constraints []UniqueIndexConstraint[T], capacity int) (*UniqueIndexGroup[T], error) {
	if len(constraints) == 0 || len(constraints) > MaxUniqueIndexGroupConstraints {
		return nil, ErrUniqueIndexGroupInvalid
	}
	if capacity < 0 {
		capacity = 0
	}
	group := &UniqueIndexGroup[T]{
		constraints: make([]UniqueIndexConstraint[T], len(constraints)),
		byName:      make(map[string]int, len(constraints)),
		owners:      make([]map[string]uint64, len(constraints)),
		entries:     make(map[uint64][]uniqueIndexGroupKey, capacity),
	}
	for index, constraint := range constraints {
		constraint.Name = strings.TrimSpace(constraint.Name)
		if constraint.Name == "" || constraint.Extract == nil {
			return nil, ErrUniqueIndexGroupInvalid
		}
		if _, exists := group.byName[constraint.Name]; exists {
			return nil, ErrUniqueIndexGroupInvalid
		}
		group.constraints[index] = constraint
		group.byName[constraint.Name] = index
		group.owners[index] = make(map[string]uint64, capacity)
	}
	return group, nil
}

// Upsert atomically inserts or replaces the indexed keys for id.
func (group *UniqueIndexGroup[T]) Upsert(id uint64, value T) error {
	if group == nil {
		return ErrUniqueIndexGroupNil
	}
	var extracted [MaxUniqueIndexGroupConstraints]uniqueIndexGroupKey
	keys := extracted[:len(group.constraints)]
	for index, constraint := range group.constraints {
		key, present := constraint.Extract(value)
		keys[index] = uniqueIndexGroupKey{value: key, present: present}
	}

	group.mu.Lock()
	defer group.mu.Unlock()
	if group.entries == nil || len(group.owners) != len(group.constraints) {
		return ErrUniqueIndexGroupInvalid
	}
	for index, key := range keys {
		if !key.present {
			continue
		}
		if owner, exists := group.owners[index][key.value]; exists && owner != id {
			return UniqueIndexViolation{Constraint: group.constraints[index].Name, Owner: owner}
		}
	}
	previous, exists := group.entries[id]
	if exists {
		for index, key := range previous {
			if key.present {
				if owner, ok := group.owners[index][key.value]; ok && owner == id {
					delete(group.owners[index], key.value)
				}
			}
		}
	}
	for index, key := range keys {
		if key.present {
			group.owners[index][key.value] = id
		}
	}
	stored := previous
	if cap(stored) < len(keys) {
		stored = make([]uniqueIndexGroupKey, len(keys))
	} else {
		stored = stored[:len(keys)]
	}
	copy(stored, keys)
	group.entries[id] = stored
	return nil
}

// Delete removes id from every unique index and reports whether it existed.
func (group *UniqueIndexGroup[T]) Delete(id uint64) bool {
	if group == nil {
		return false
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	previous, exists := group.entries[id]
	if !exists {
		return false
	}
	for index, key := range previous {
		if key.present {
			if owner, ok := group.owners[index][key.value]; ok && owner == id {
				delete(group.owners[index], key.value)
			}
		}
	}
	delete(group.entries, id)
	return true
}

// Lookup returns the owning row ID for one constraint key.
func (group *UniqueIndexGroup[T]) Lookup(constraint, key string) (uint64, bool, error) {
	if group == nil {
		return 0, false, ErrUniqueIndexGroupNil
	}
	constraint = strings.TrimSpace(constraint)
	group.mu.RLock()
	index, exists := group.byName[constraint]
	if !exists {
		group.mu.RUnlock()
		return 0, false, ErrUniqueIndexGroupConstraintUnknown
	}
	owner, found := group.owners[index][key]
	group.mu.RUnlock()
	return owner, found, nil
}

// ConstraintNames returns the stable declaration order of the unique indexes.
func (group *UniqueIndexGroup[T]) ConstraintNames() []string {
	if group == nil {
		return nil
	}
	group.mu.RLock()
	defer group.mu.RUnlock()
	names := make([]string, len(group.constraints))
	for index, constraint := range group.constraints {
		names[index] = constraint.Name
	}
	return names
}

// Len returns the number of rows with at least one indexed entry.
func (group *UniqueIndexGroup[T]) Len() int {
	if group == nil {
		return 0
	}
	group.mu.RLock()
	defer group.mu.RUnlock()
	return len(group.entries)
}

// Clear removes all rows while retaining the constraint definitions and map
// capacity for reuse.
func (group *UniqueIndexGroup[T]) Clear() {
	if group == nil {
		return
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	group.entries = make(map[uint64][]uniqueIndexGroupKey, len(group.entries))
	for index := range group.owners {
		group.owners[index] = make(map[string]uint64, len(group.owners[index]))
	}
}
