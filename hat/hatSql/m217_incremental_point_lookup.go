package hatSql

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
)

var (
	ErrIncrementalPointLookupNil                  = errors.New("incremental point lookup index is nil")
	ErrIncrementalPointLookupKeyRequired          = errors.New("incremental point lookup index key function is required")
	ErrIncrementalPointLookupRowRequired          = errors.New("incremental point lookup row is required for a new key")
	ErrIncrementalPointLookupRowConflict          = errors.New("incremental point lookup row conflicts with existing key")
	ErrIncrementalPointLookupNegativeMultiplicity = errors.New("incremental point lookup multiplicity became negative")
	ErrIncrementalPointLookupOverflow             = errors.New("incremental point lookup multiplicity overflowed")
)

// IncrementalPointLookupKeyFunc extracts the exact posting key for one
// maintained view row. The returned string may be empty; an empty value is a
// valid point-lookup key.
type IncrementalPointLookupKeyFunc func(Row) (string, error)

// IncrementalPointLookupDefinition configures one maintained point-lookup
// arrangement over complete view rows.
type IncrementalPointLookupDefinition struct {
	IndexKey IncrementalPointLookupKeyFunc
}

type incrementalPointLookupEntry struct {
	key      string
	indexKey string
	time     uint64
	row      Row
	count    int64
}

type incrementalPointLookupPendingEntry struct {
	active   bool
	key      string
	indexKey string
	time     uint64
	row      Row
	count    int64
}

// IncrementalPointLookup maintains a complete-row posting arrangement under
// signed differential updates. Each active row is retained once, while the
// posting buckets point at that retained row. Lookup returns differential rows
// with their current multiplicity, so duplicate view rows do not require
// duplicate row-map storage.
//
// Apply validates the complete batch before committing any state. Row maps
// are cloned when inserted and when returned by Lookup or Snapshot. The index
// key callback is invoked while the index write lock is held and must not call
// back into the same index.
type IncrementalPointLookup struct {
	mu       sync.RWMutex
	indexKey IncrementalPointLookupKeyFunc
	entries  map[string]*incrementalPointLookupEntry
	buckets  map[string]map[string]*incrementalPointLookupEntry
}

// NewIncrementalPointLookup creates an empty maintained point-lookup index.
func NewIncrementalPointLookup(definition IncrementalPointLookupDefinition) (*IncrementalPointLookup, error) {
	if definition.IndexKey == nil {
		return nil, ErrIncrementalPointLookupKeyRequired
	}
	return &IncrementalPointLookup{
		indexKey: definition.IndexKey,
		entries:  make(map[string]*incrementalPointLookupEntry),
		buckets:  make(map[string]map[string]*incrementalPointLookupEntry),
	}, nil
}

// Apply validates and applies signed row updates atomically. Positive updates
// for a new row key must include Row. A positive update for an existing key
// may omit Row to add multiplicity; when Row is present it must match the
// retained complete row and its extracted index key.
func (index *IncrementalPointLookup) Apply(updates []DifferentialRow) error {
	if index == nil {
		return ErrIncrementalPointLookupNil
	}
	if len(updates) == 0 {
		return nil
	}

	index.mu.Lock()
	defer index.mu.Unlock()
	pending := make(map[string]incrementalPointLookupPendingEntry, len(updates))
	for updateIndex, update := range updates {
		if update.Key == "" {
			return fmt.Errorf("incremental point lookup update %d: differential row key is required", updateIndex)
		}
		if update.Diff == 0 {
			continue
		}
		entry, exists := pending[update.Key]
		if !exists {
			if current := index.entries[update.Key]; current != nil {
				entry = incrementalPointLookupPendingEntry{
					active:   true,
					key:      current.key,
					indexKey: current.indexKey,
					time:     current.time,
					row:      current.row,
					count:    current.count,
				}
			}
		}

		if update.Diff > 0 {
			if !entry.active {
				if update.Row == nil {
					return fmt.Errorf("incremental point lookup update %d key %q: %w", updateIndex, update.Key, ErrIncrementalPointLookupRowRequired)
				}
				postingKey, err := index.indexKey(update.Row)
				if err != nil {
					return fmt.Errorf("incremental point lookup update %d key %q: index key: %w", updateIndex, update.Key, err)
				}
				entry = incrementalPointLookupPendingEntry{
					active:   true,
					key:      update.Key,
					indexKey: postingKey,
					time:     update.Time,
					row:      cloneDifferentialRow(update.Row),
				}
			} else if update.Row != nil {
				postingKey, err := index.indexKey(update.Row)
				if err != nil {
					return fmt.Errorf("incremental point lookup update %d key %q: index key: %w", updateIndex, update.Key, err)
				}
				if postingKey != entry.indexKey || !reflect.DeepEqual(update.Row, entry.row) {
					return fmt.Errorf("incremental point lookup update %d key %q: %w", updateIndex, update.Key, ErrIncrementalPointLookupRowConflict)
				}
			}
			next, ok := addDifferentialCounts(entry.count, update.Diff)
			if !ok {
				return fmt.Errorf("incremental point lookup update %d key %q: %w", updateIndex, update.Key, ErrIncrementalPointLookupOverflow)
			}
			entry.count = next
		} else {
			decrement := incrementalPointLookupMagnitude(update.Diff)
			if !entry.active || decrement > uint64(entry.count) {
				return fmt.Errorf("incremental point lookup update %d key %q: %w", updateIndex, update.Key, ErrIncrementalPointLookupNegativeMultiplicity)
			}
			entry.count -= int64(decrement)
			if entry.count == 0 {
				entry = incrementalPointLookupPendingEntry{}
			}
		}
		pending[update.Key] = entry
	}

	for key, entry := range pending {
		current := index.entries[key]
		if !entry.active {
			if current != nil {
				index.removeEntryLocked(current)
			}
			continue
		}
		if current == nil {
			current = &incrementalPointLookupEntry{key: entry.key}
			index.entries[key] = current
		} else if current.indexKey != entry.indexKey {
			index.removeBucketEntryLocked(current)
		}
		current.indexKey = entry.indexKey
		current.time = entry.time
		current.row = entry.row
		current.count = entry.count
		bucket := index.buckets[current.indexKey]
		if bucket == nil {
			bucket = make(map[string]*incrementalPointLookupEntry)
			index.buckets[current.indexKey] = bucket
		}
		bucket[current.key] = current
	}
	return nil
}

// Lookup returns all complete rows currently indexed by indexKey. Results are
// sorted by stable row key and each returned row map is independent from the
// retained state.
func (index *IncrementalPointLookup) Lookup(indexKey string) []DifferentialRow {
	if index == nil {
		return nil
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	return index.lookupLocked(indexKey)
}

// Snapshot returns every active indexed row in stable row-key order.
func (index *IncrementalPointLookup) Snapshot() []DifferentialRow {
	if index == nil {
		return nil
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	if len(index.entries) == 0 {
		return nil
	}
	keys := make([]string, 0, len(index.entries))
	for key := range index.entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]DifferentialRow, 0, len(keys))
	for _, key := range keys {
		result = append(result, incrementalPointLookupRow(index.entries[key]))
	}
	return result
}

// AllRows is an alias for Snapshot for callers that use relation-style
// naming.
func (index *IncrementalPointLookup) AllRows() []DifferentialRow {
	return index.Snapshot()
}

// Len reports the number of active complete rows, excluding multiplicity.
func (index *IncrementalPointLookup) Len() int {
	if index == nil {
		return 0
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	return len(index.entries)
}

// BucketCount reports the number of distinct point-lookup keys with active
// rows.
func (index *IncrementalPointLookup) BucketCount() int {
	if index == nil {
		return 0
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	return len(index.buckets)
}

func (index *IncrementalPointLookup) lookupLocked(indexKey string) []DifferentialRow {
	bucket := index.buckets[indexKey]
	if len(bucket) == 0 {
		return nil
	}
	keys := make([]string, 0, len(bucket))
	for key := range bucket {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]DifferentialRow, 0, len(keys))
	for _, key := range keys {
		result = append(result, incrementalPointLookupRow(bucket[key]))
	}
	return result
}

func incrementalPointLookupRow(entry *incrementalPointLookupEntry) DifferentialRow {
	return DifferentialRow{
		Key:  entry.key,
		Time: entry.time,
		Diff: entry.count,
		Row:  cloneDifferentialRow(entry.row),
	}
}

func (index *IncrementalPointLookup) removeEntryLocked(entry *incrementalPointLookupEntry) {
	delete(index.entries, entry.key)
	index.removeBucketEntryLocked(entry)
}

func (index *IncrementalPointLookup) removeBucketEntryLocked(entry *incrementalPointLookupEntry) {
	bucket := index.buckets[entry.indexKey]
	if bucket == nil {
		return
	}
	delete(bucket, entry.key)
	if len(bucket) == 0 {
		delete(index.buckets, entry.indexKey)
	}
}

func incrementalPointLookupMagnitude(diff int64) uint64 {
	return uint64(-(diff + 1)) + 1
}

// String provides a compact state summary useful in diagnostics without
// exposing retained row contents.
func (index *IncrementalPointLookup) String() string {
	if index == nil {
		return "IncrementalPointLookup<nil>"
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	return fmt.Sprintf("IncrementalPointLookup{entries=%d,buckets=%d}", len(index.entries), len(index.buckets))
}
