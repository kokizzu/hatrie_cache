package hatDataStructure

import (
	"fmt"
	"sync"
)

// StringMultikeyIndexOptions controls the compact string multikey index. A
// nonpositive limit means unlimited, preserving the original adapter contract.
type StringMultikeyIndexOptions struct {
	MaxKeysPerItem int
	MaxItems       int
}

type stringMultikeyEntry struct {
	first  string
	second string
	extra  []string
	count  uint32
}

// StringMultikeyIndex is a compact convenience index for records represented
// only by string keys. The first two keys are stored inline; only larger key
// sets allocate an overflow slice. Empty key slices remove an item.
type StringMultikeyIndex struct {
	mu             sync.RWMutex
	maxKeysPerItem int
	maxItems       int
	entries        map[uint64]stringMultikeyEntry
	postings       map[string]u64PostingList
}

// NewStringMultikeyIndex creates a bounded string multikey index.
func NewStringMultikeyIndex(options StringMultikeyIndexOptions) *StringMultikeyIndex {
	if options.MaxItems < 0 {
		options.MaxItems = 0
	}
	return &StringMultikeyIndex{
		maxKeysPerItem: options.MaxKeysPerItem,
		maxItems:       options.MaxItems,
		entries:        make(map[uint64]stringMultikeyEntry, options.MaxItems),
		postings:       make(map[string]u64PostingList),
	}
}

// Set replaces the keys for id. Duplicate keys are indexed once; an empty
// key slice removes id.
func (index *StringMultikeyIndex) Set(id uint64, keys []string) error {
	if index == nil {
		return fmt.Errorf("string multikey index is nil")
	}
	if len(keys) == 0 {
		index.Delete(id)
		return nil
	}
	if index.maxKeysPerItem > 0 && len(keys) > index.maxKeysPerItem {
		return fmt.Errorf("string multikey key limit exceeded: maximum %d", index.maxKeysPerItem)
	}

	var stackKeys [multiKeyIndexStackKeys]string
	uniqueKeys := stackKeys[:0]
	for _, key := range keys {
		duplicate := false
		for _, existingKey := range uniqueKeys {
			if existingKey == key {
				duplicate = true
				break
			}
		}
		if !duplicate {
			uniqueKeys = append(uniqueKeys, key)
		}
	}

	index.mu.Lock()
	defer index.mu.Unlock()
	index.ensureStringInitializedLocked()
	current, exists := index.entries[id]
	if !exists && index.maxItems > 0 && len(index.entries) >= index.maxItems {
		return fmt.Errorf("string multikey index item limit exceeded: maximum %d", index.maxItems)
	}
	if exists && stringMultikeyEntryKeySetsEqual(current, uniqueKeys) {
		return nil
	}

	forEachStringMultikeyEntryKey(current, func(key string) {
		index.removeStringKeyLocked(key, id)
	})
	entry := stringMultikeyEntryFromKeys(uniqueKeys, current, exists)
	for _, key := range uniqueKeys {
		index.insertStringPostingLocked(key, id)
	}
	index.entries[id] = entry
	return nil
}

// Delete removes id and reports whether it was present.
func (index *StringMultikeyIndex) Delete(id uint64) bool {
	if index == nil {
		return false
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	entry, exists := index.entries[id]
	if !exists {
		return false
	}
	delete(index.entries, id)
	forEachStringMultikeyEntryKey(entry, func(key string) {
		index.removeStringKeyLocked(key, id)
	})
	return true
}

// Lookup returns sorted stable IDs for key, reusing dst when possible.
func (index *StringMultikeyIndex) Lookup(key string, dst []uint64) []uint64 {
	dst = dst[:0]
	if index == nil {
		return dst
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	posting, ok := index.postings[key]
	if !ok {
		return dst
	}
	return posting.values(dst)
}

// Contains reports whether id is indexed under key.
func (index *StringMultikeyIndex) Contains(key string, id uint64) bool {
	if index == nil {
		return false
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	posting, ok := index.postings[key]
	if !ok {
		return false
	}
	if posting.rest == nil {
		return posting.first == id
	}
	if posting.first == id || posting.rest.first == id {
		return true
	}
	for _, candidate := range posting.rest.rest {
		if candidate == id {
			return true
		}
	}
	return false
}

// Len returns the number of indexed items.
func (index *StringMultikeyIndex) Len() int {
	if index == nil {
		return 0
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	return len(index.entries)
}

// KeyCount returns the number of distinct keys with postings.
func (index *StringMultikeyIndex) KeyCount() int {
	if index == nil {
		return 0
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	return len(index.postings)
}

func (index *StringMultikeyIndex) ensureStringInitializedLocked() {
	if index.entries == nil {
		index.entries = make(map[uint64]stringMultikeyEntry)
	}
	if index.postings == nil {
		index.postings = make(map[string]u64PostingList)
	}
}

func (index *StringMultikeyIndex) removeStringKeyLocked(key string, id uint64) {
	posting, ok := index.postings[key]
	if !ok {
		return
	}
	next, removed, empty := posting.removeSorted(id)
	if !removed {
		return
	}
	if empty {
		delete(index.postings, key)
		return
	}
	index.postings[key] = next
}

func (index *StringMultikeyIndex) insertStringPostingLocked(key string, id uint64) {
	if posting, ok := index.postings[key]; ok {
		index.postings[key] = posting.insertSorted(id)
		return
	}
	index.postings[key] = newU64PostingList(id)
}

func stringMultikeyEntryFromKeys(keys []string, current stringMultikeyEntry, reuse bool) stringMultikeyEntry {
	entry := stringMultikeyEntry{count: uint32(len(keys))}
	if len(keys) > 0 {
		entry.first = keys[0]
	}
	if len(keys) > 1 {
		entry.second = keys[1]
	}
	if len(keys) > 2 {
		if reuse && len(current.extra) == len(keys)-2 {
			entry.extra = current.extra
		} else {
			entry.extra = make([]string, len(keys)-2)
		}
		copy(entry.extra, keys[2:])
	}
	return entry
}

func forEachStringMultikeyEntryKey(entry stringMultikeyEntry, visit func(string)) {
	if entry.count == 0 {
		return
	}
	visit(entry.first)
	if entry.count > 1 {
		visit(entry.second)
	}
	for _, key := range entry.extra {
		visit(key)
	}
}

func stringMultikeyEntryKeySetsEqual(entry stringMultikeyEntry, keys []string) bool {
	if int(entry.count) != len(keys) {
		return false
	}
	for _, key := range keys {
		found := false
		forEachStringMultikeyEntryKey(entry, func(existing string) {
			if existing == key {
				found = true
			}
		})
		if !found {
			return false
		}
	}
	return true
}
