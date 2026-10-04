package hatDataStructure

import (
	"errors"
	"fmt"
	"sync"
)

const (
	// DefaultUniqueConstraintMaxKeyBytes bounds one key when no limit is supplied.
	DefaultUniqueConstraintMaxKeyBytes = 1 << 20
	maxUniqueConstraintCount           = 64
)

var (
	// ErrUniqueConstraintInvalidOptions reports an invalid constructor option.
	ErrUniqueConstraintInvalidOptions = errors.New("invalid unique constraint options")
	// ErrUniqueConstraintSetNil reports a call on a nil set.
	ErrUniqueConstraintSetNil = errors.New("nil unique constraint set")
	// ErrUniqueConstraintArity reports a key count different from the constraint count.
	ErrUniqueConstraintArity = errors.New("unique constraint key count mismatch")
	// ErrUniqueConstraintConflict reports a key already owned by another row.
	ErrUniqueConstraintConflict = errors.New("unique constraint conflict")
	// ErrUniqueConstraintKeyTooLarge reports a key over the configured byte limit.
	ErrUniqueConstraintKeyTooLarge = errors.New("unique constraint key too large")
)

// UniqueConstraintSetOptions configures an atomic set of named unique keys.
// ConstraintNames define the key namespace for each position in Upsert.
type UniqueConstraintSetOptions struct {
	ConstraintNames []string
	Capacity        int
	MaxKeyBytes     int
}

// UniqueConstraintConflictError identifies the constraint and row that won a key.
// The conflicting key is intentionally omitted so callers do not accidentally
// include sensitive values in logs or public errors.
type UniqueConstraintConflictError struct {
	Constraint string
	Owner      uint64
}

func (err *UniqueConstraintConflictError) Error() string {
	return fmt.Sprintf("unique constraint %q is owned by row %d", err.Constraint, err.Owner)
}

func (err *UniqueConstraintConflictError) Unwrap() error {
	return ErrUniqueConstraintConflict
}

// UniqueConstraintSet atomically reserves one unique key per named constraint
// for each row. It is useful when a row has several unique projections that
// must be admitted or rejected as one operation.
//
// Upsert checks every constraint under one lock before changing any map. Keys
// are retained as string headers in a flat row arena; deleted row slots are
// reused to keep steady-state updates allocation-free.
type UniqueConstraintSet struct {
	mu sync.RWMutex

	names       []string
	owners      []map[string]uint64
	rows        map[uint64]int
	rowKeys     []string
	freeSlots   []int
	maxKeyBytes int
}

// NewUniqueConstraintSet creates an atomic set of named unique constraints.
func NewUniqueConstraintSet(options UniqueConstraintSetOptions) (*UniqueConstraintSet, error) {
	if len(options.ConstraintNames) == 0 || len(options.ConstraintNames) > maxUniqueConstraintCount || options.Capacity < 0 || options.MaxKeyBytes < 0 {
		return nil, ErrUniqueConstraintInvalidOptions
	}
	if options.MaxKeyBytes == 0 {
		options.MaxKeyBytes = DefaultUniqueConstraintMaxKeyBytes
	}
	maxInt := int(^uint(0) >> 1)
	if options.Capacity > maxInt/len(options.ConstraintNames) {
		return nil, ErrUniqueConstraintInvalidOptions
	}

	names := append([]string(nil), options.ConstraintNames...)
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if name == "" {
			return nil, ErrUniqueConstraintInvalidOptions
		}
		if _, exists := seen[name]; exists {
			return nil, ErrUniqueConstraintInvalidOptions
		}
		seen[name] = struct{}{}
	}

	owners := make([]map[string]uint64, len(names))
	for index := range owners {
		owners[index] = make(map[string]uint64, options.Capacity)
	}
	return &UniqueConstraintSet{
		names:       names,
		owners:      owners,
		rows:        make(map[uint64]int, options.Capacity),
		rowKeys:     make([]string, 0, options.Capacity*len(names)),
		maxKeyBytes: options.MaxKeyBytes,
	}, nil
}

// Upsert atomically reserves keys for id. On conflict, no reservation is
// changed and the returned error identifies the already owning row.
func (set *UniqueConstraintSet) Upsert(id uint64, keys []string) error {
	if set == nil {
		return ErrUniqueConstraintSetNil
	}
	if len(keys) != len(set.names) {
		return ErrUniqueConstraintArity
	}
	for _, key := range keys {
		if len(key) > set.maxKeyBytes {
			return ErrUniqueConstraintKeyTooLarge
		}
	}

	set.mu.Lock()
	defer set.mu.Unlock()

	slot, exists := set.rows[id]
	for index, key := range keys {
		if owner, found := set.owners[index][key]; found && owner != id {
			return &UniqueConstraintConflictError{Constraint: set.names[index], Owner: owner}
		}
	}

	if exists {
		offset := slot * len(set.names)
		oldKeys := set.rowKeys[offset : offset+len(set.names)]
		unchanged := true
		for index, key := range keys {
			if oldKeys[index] != key {
				unchanged = false
				break
			}
		}
		if unchanged {
			return nil
		}
		for index, oldKey := range oldKeys {
			if set.owners[index][oldKey] == id {
				delete(set.owners[index], oldKey)
			}
		}
		copy(oldKeys, keys)
	} else if len(set.freeSlots) > 0 {
		last := len(set.freeSlots) - 1
		slot = set.freeSlots[last]
		set.freeSlots = set.freeSlots[:last]
		offset := slot * len(set.names)
		copy(set.rowKeys[offset:offset+len(set.names)], keys)
		set.rows[id] = slot
	} else {
		slot = len(set.rowKeys) / len(set.names)
		set.rowKeys = append(set.rowKeys, keys...)
		set.rows[id] = slot
	}

	for index, key := range keys {
		set.owners[index][key] = id
	}
	return nil
}

// Delete releases every key owned by id. It returns false when id is absent.
func (set *UniqueConstraintSet) Delete(id uint64) bool {
	if set == nil {
		return false
	}
	set.mu.Lock()
	defer set.mu.Unlock()

	slot, exists := set.rows[id]
	if !exists {
		return false
	}
	offset := slot * len(set.names)
	keys := set.rowKeys[offset : offset+len(set.names)]
	for index, key := range keys {
		if set.owners[index][key] == id {
			delete(set.owners[index], key)
		}
		keys[index] = ""
	}
	delete(set.rows, id)
	set.freeSlots = append(set.freeSlots, slot)
	return true
}

// Owner returns the row owning key in a named constraint.
func (set *UniqueConstraintSet) Owner(constraint, key string) (uint64, bool) {
	if set == nil {
		return 0, false
	}
	set.mu.RLock()
	defer set.mu.RUnlock()
	for index, name := range set.names {
		if name == constraint {
			owner, ok := set.owners[index][key]
			return owner, ok
		}
	}
	return 0, false
}

// OwnerAt returns the row owning key in the zero-based constraint position.
func (set *UniqueConstraintSet) OwnerAt(index int, key string) (uint64, bool) {
	if set == nil || index < 0 || index >= len(set.names) {
		return 0, false
	}
	set.mu.RLock()
	defer set.mu.RUnlock()
	owner, ok := set.owners[index][key]
	return owner, ok
}

// ConstraintNames returns an independent copy of the configured names.
func (set *UniqueConstraintSet) ConstraintNames() []string {
	if set == nil {
		return nil
	}
	set.mu.RLock()
	defer set.mu.RUnlock()
	return append([]string(nil), set.names...)
}

// Len returns the number of rows with at least one reservation.
func (set *UniqueConstraintSet) Len() int {
	if set == nil {
		return 0
	}
	set.mu.RLock()
	defer set.mu.RUnlock()
	return len(set.rows)
}

// ConstraintCount returns the number of unique namespaces.
func (set *UniqueConstraintSet) ConstraintCount() int {
	if set == nil {
		return 0
	}
	return len(set.names)
}
