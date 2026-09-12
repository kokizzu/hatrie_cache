package hatDataStructure

import (
	"fmt"
	"sort"
	"sync"
)

// StringMultikeyIndexOptions bounds the state held by a
// StringMultikeyIndex. A nonpositive limit means unlimited.
type StringMultikeyIndexOptions struct {
	MaxKeysPerItem int
	MaxItems       int
}

// StringMultikeyIndex maps each string key to sorted item IDs. It is intended
// for read-heavy array-membership indexes: posting lists are compact slices,
// while the reverse map makes replacement and deletion exact.
type StringMultikeyIndex struct {
	mu      sync.RWMutex
	options StringMultikeyIndexOptions
	byKey   map[string][]uint64
	byID    map[uint64][]string
}

// NewStringMultikeyIndex creates an empty bounded multikey index.
func NewStringMultikeyIndex(options StringMultikeyIndexOptions) *StringMultikeyIndex {
	return &StringMultikeyIndex{
		options: options,
		byKey:   make(map[string][]uint64),
		byID:    make(map[uint64][]string),
	}
}

// Set replaces all keys associated with id. Input keys are deduplicated and
// sorted before mutation. Validation and capacity checks happen before any
// existing state is changed, so rejected updates are atomic.
func (index *StringMultikeyIndex) Set(id uint64, keys []string) error {
	if index == nil {
		return fmt.Errorf("string multikey index is nil")
	}
	normalized, err := normalizeStringMultikeyKeys(keys, index.options.MaxKeysPerItem)
	if err != nil {
		return err
	}

	index.mu.Lock()
	defer index.mu.Unlock()
	old, exists := index.byID[id]
	if !exists && index.options.MaxItems > 0 && len(index.byID) >= index.options.MaxItems {
		return fmt.Errorf("string multikey index item limit exceeded: maximum %d", index.options.MaxItems)
	}
	if stringMultikeyKeysEqual(old, normalized) {
		return nil
	}
	for _, key := range old {
		index.removePosting(key, id)
	}
	if len(normalized) == 0 {
		delete(index.byID, id)
		return nil
	}
	for _, key := range normalized {
		index.byKey[key] = insertStringMultikeyID(index.byKey[key], id)
	}
	index.byID[id] = normalized
	return nil
}

// Delete removes id and all of its postings. It reports whether id existed.
func (index *StringMultikeyIndex) Delete(id uint64) bool {
	if index == nil {
		return false
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	keys, exists := index.byID[id]
	if !exists {
		return false
	}
	for _, key := range keys {
		index.removePosting(key, id)
	}
	delete(index.byID, id)
	return true
}

// Lookup appends sorted matching item IDs to dst and returns the resulting
// slice. Passing a reusable destination avoids an allocation on the caller's
// hot path; a nil destination allocates only when matches exist.
func (index *StringMultikeyIndex) Lookup(key string, dst []uint64) []uint64 {
	if index == nil {
		if dst != nil {
			return dst[:0]
		}
		return nil
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	ids := index.byKey[key]
	if len(ids) == 0 {
		if dst != nil {
			return dst[:0]
		}
		return nil
	}
	return append(dst[:0], ids...)
}

// Contains reports whether id is indexed under key.
func (index *StringMultikeyIndex) Contains(key string, id uint64) bool {
	if index == nil {
		return false
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	ids := index.byKey[key]
	position := sort.Search(len(ids), func(position int) bool { return ids[position] >= id })
	return position < len(ids) && ids[position] == id
}

// Len returns the number of items with at least one indexed key.
func (index *StringMultikeyIndex) Len() int {
	if index == nil {
		return 0
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	return len(index.byID)
}

// KeyCount returns the number of distinct keys with at least one posting.
func (index *StringMultikeyIndex) KeyCount() int {
	if index == nil {
		return 0
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	return len(index.byKey)
}

func normalizeStringMultikeyKeys(keys []string, maxKeys int) ([]string, error) {
	if maxKeys > 0 && len(keys) > maxKeys {
		return nil, fmt.Errorf("string multikey key limit exceeded: maximum %d", maxKeys)
	}
	if len(keys) == 0 {
		return nil, nil
	}
	normalized := append([]string(nil), keys...)
	sort.Strings(normalized)
	unique := normalized[:0]
	for _, key := range normalized {
		if len(unique) == 0 || unique[len(unique)-1] != key {
			unique = append(unique, key)
		}
	}
	return unique, nil
}

func stringMultikeyKeysEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func insertStringMultikeyID(ids []uint64, id uint64) []uint64 {
	position := sort.Search(len(ids), func(position int) bool { return ids[position] >= id })
	if position < len(ids) && ids[position] == id {
		return ids
	}
	ids = append(ids, 0)
	copy(ids[position+1:], ids[position:])
	ids[position] = id
	return ids
}

func (index *StringMultikeyIndex) removePosting(key string, id uint64) {
	ids := index.byKey[key]
	position := sort.Search(len(ids), func(position int) bool { return ids[position] >= id })
	if position >= len(ids) || ids[position] != id {
		return
	}
	copy(ids[position:], ids[position+1:])
	ids = ids[:len(ids)-1]
	if len(ids) == 0 {
		delete(index.byKey, key)
		return
	}
	index.byKey[key] = ids
}
