package hatDataStructure

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"unicode/utf8"
)

const (
	// DefaultCrossIndexUniqueMaxEntries bounds the number of records unless a
	// caller provides a smaller limit.
	DefaultCrossIndexUniqueMaxEntries = 1 << 20
	maxCrossIndexUniqueEntries        = 1 << 24
	maxCrossIndexUniqueIndexes        = 64
	maxCrossIndexUniqueNameBytes      = 128
)

var (
	ErrCrossIndexUniqueNil      = errors.New("cross-index unique set is nil")
	ErrCrossIndexUniqueInvalid  = errors.New("invalid cross-index unique definition")
	ErrCrossIndexUniqueConflict = errors.New("cross-index unique conflict")
	ErrCrossIndexUniqueLimit    = errors.New("cross-index unique limit exceeded")
)

// CrossIndexUniqueDefinition describes one named unique key derived from a
// record. All definitions participate in the same atomic Upsert operation.
type CrossIndexUniqueDefinition[T any] struct {
	Name string
	Key  func(T) string
}

// CrossIndexUniqueOptions controls memory reservation and the entry limit.
// Capacity is only a map allocation hint; MaxEntries is the hard admission
// limit. Zero selects the bounded default for either field.
type CrossIndexUniqueOptions struct {
	Capacity   int
	MaxEntries int
}

type crossIndexUniqueIndex[T any] struct {
	name   string
	key    func(T) string
	owners map[string]uint64
}

type crossIndexUniqueEntry[T any] struct {
	value T
	keys  []string
}

// CrossIndexUniqueSet atomically maintains several unique indexes for the
// same set of records. It is useful when a record must be unique by more than
// one identity, such as both email and username.
//
// The set owns its index maps and serializes updates with one lock. Failed
// updates perform no mutation. Definitions are immutable after construction.
type CrossIndexUniqueSet[T any] struct {
	mu          sync.RWMutex
	indexes     []crossIndexUniqueIndex[T]
	indexByName map[string]int
	entries     map[uint64]crossIndexUniqueEntry[T]
	maxEntries  int
}

// NewCrossIndexUniqueSet constructs an atomic multi-index uniqueness set.
func NewCrossIndexUniqueSet[T any](definitions []CrossIndexUniqueDefinition[T], options CrossIndexUniqueOptions) (*CrossIndexUniqueSet[T], error) {
	if len(definitions) == 0 {
		return nil, fmt.Errorf("%w: at least one index is required", ErrCrossIndexUniqueInvalid)
	}
	if len(definitions) > maxCrossIndexUniqueIndexes {
		return nil, fmt.Errorf("%w: at most %d indexes are supported", ErrCrossIndexUniqueLimit, maxCrossIndexUniqueIndexes)
	}
	if options.Capacity < 0 {
		return nil, fmt.Errorf("%w: capacity cannot be negative", ErrCrossIndexUniqueInvalid)
	}

	maxEntries := options.MaxEntries
	if maxEntries == 0 {
		maxEntries = DefaultCrossIndexUniqueMaxEntries
	}
	if maxEntries < 1 || maxEntries > maxCrossIndexUniqueEntries {
		return nil, fmt.Errorf("%w: max entries must be between 1 and %d", ErrCrossIndexUniqueLimit, maxCrossIndexUniqueEntries)
	}
	if options.Capacity > maxEntries {
		return nil, fmt.Errorf("%w: capacity cannot exceed max entries", ErrCrossIndexUniqueLimit)
	}

	indexes := make([]crossIndexUniqueIndex[T], len(definitions))
	indexByName := make(map[string]int, len(definitions))
	for i, definition := range definitions {
		name := strings.TrimSpace(definition.Name)
		if name == "" || len(name) > maxCrossIndexUniqueNameBytes || !utf8.ValidString(name) || definition.Key == nil {
			return nil, fmt.Errorf("%w: index %q needs a valid name and key function", ErrCrossIndexUniqueInvalid, definition.Name)
		}
		if _, exists := indexByName[name]; exists {
			return nil, fmt.Errorf("%w: duplicate index name %q", ErrCrossIndexUniqueInvalid, name)
		}
		indexByName[name] = i
		indexes[i] = crossIndexUniqueIndex[T]{
			name:   name,
			key:    definition.Key,
			owners: make(map[string]uint64, options.Capacity),
		}
	}

	return &CrossIndexUniqueSet[T]{
		indexes:     indexes,
		indexByName: indexByName,
		entries:     make(map[uint64]crossIndexUniqueEntry[T], options.Capacity),
		maxEntries:  maxEntries,
	}, nil
}

// Upsert inserts or replaces id after checking every unique index. A conflict
// or capacity error leaves the previous record and all index owners unchanged.
func (set *CrossIndexUniqueSet[T]) Upsert(id uint64, value T) error {
	if set == nil {
		return ErrCrossIndexUniqueNil
	}

	var keyBuffer [maxCrossIndexUniqueIndexes]string
	for i, index := range set.indexes {
		keyBuffer[i] = index.key(value)
	}

	set.mu.Lock()
	defer set.mu.Unlock()

	existing, exists := set.entries[id]
	if !exists && len(set.entries) >= set.maxEntries {
		return ErrCrossIndexUniqueLimit
	}
	for i, key := range keyBuffer[:len(set.indexes)] {
		if owner, ok := set.indexes[i].owners[key]; ok && owner != id {
			return ErrCrossIndexUniqueConflict
		}
	}
	if exists && len(existing.keys) == len(set.indexes) {
		keysUnchanged := true
		for i, key := range keyBuffer[:len(set.indexes)] {
			if existing.keys[i] != key {
				keysUnchanged = false
				break
			}
		}
		if keysUnchanged {
			set.entries[id] = crossIndexUniqueEntry[T]{value: value, keys: existing.keys}
			return nil
		}
	}

	storedKeys := existing.keys
	if len(storedKeys) != len(set.indexes) {
		storedKeys = make([]string, len(set.indexes))
	}
	if exists {
		for i, oldKey := range existing.keys {
			if owner, ok := set.indexes[i].owners[oldKey]; ok && owner == id {
				delete(set.indexes[i].owners, oldKey)
			}
		}
	}
	copy(storedKeys, keyBuffer[:len(set.indexes)])
	for i, key := range storedKeys {
		set.indexes[i].owners[key] = id
	}
	set.entries[id] = crossIndexUniqueEntry[T]{value: value, keys: storedKeys}
	return nil
}

// Delete removes id and all of its unique keys. It reports whether id existed.
func (set *CrossIndexUniqueSet[T]) Delete(id uint64) bool {
	if set == nil {
		return false
	}
	set.mu.Lock()
	defer set.mu.Unlock()

	entry, ok := set.entries[id]
	if !ok {
		return false
	}
	for i, key := range entry.keys {
		if owner, exists := set.indexes[i].owners[key]; exists && owner == id {
			delete(set.indexes[i].owners, key)
		}
	}
	delete(set.entries, id)
	return true
}

// LookupOwner returns the record id currently owning key in indexName.
func (set *CrossIndexUniqueSet[T]) LookupOwner(indexName, key string) (uint64, bool) {
	if set == nil {
		return 0, false
	}
	set.mu.RLock()
	defer set.mu.RUnlock()

	indexNumber, ok := set.indexByName[indexName]
	if !ok {
		return 0, false
	}
	owner, ok := set.indexes[indexNumber].owners[key]
	return owner, ok
}

// IndexNames returns the configured index names in definition order.
func (set *CrossIndexUniqueSet[T]) IndexNames() []string {
	if set == nil {
		return nil
	}
	set.mu.RLock()
	defer set.mu.RUnlock()

	names := make([]string, len(set.indexes))
	for i, index := range set.indexes {
		names[i] = index.name
	}
	return names
}

// Len returns the number of records currently admitted to the set.
func (set *CrossIndexUniqueSet[T]) Len() int {
	if set == nil {
		return 0
	}
	set.mu.RLock()
	defer set.mu.RUnlock()
	return len(set.entries)
}
