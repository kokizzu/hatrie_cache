package hatMetrics

import (
	"errors"
	"sort"
	"strings"
	"sync"
)

var ErrCollectionNameRequired = errors.New("hatriecache: collection name is required")

// CollectionMetrics is a point-in-time size gauge and compaction counter for
// one collection.
type CollectionMetrics struct {
	Collection       string `json:"collection"`
	Entries          uint64 `json:"entries"`
	Bytes            uint64 `json:"bytes"`
	CompactionsTotal uint64 `json:"compactions_total"`
}

// CollectionMetricsRegistry stores collection size and compaction metrics. It
// is safe for concurrent reporters and snapshot readers.
type CollectionMetricsRegistry struct {
	mu          sync.RWMutex
	collections map[string]CollectionMetrics
}

// NewCollectionMetricsRegistry creates an empty collection metrics registry.
func NewCollectionMetricsRegistry() *CollectionMetricsRegistry {
	return &CollectionMetricsRegistry{collections: make(map[string]CollectionMetrics)}
}

// SetSize records the current entry count and byte size for collection. Size
// values are gauges and may increase or decrease between observations.
func (registry *CollectionMetricsRegistry) SetSize(collection string, entries, bytes uint64) error {
	collection = strings.TrimSpace(collection)
	if collection == "" {
		return ErrCollectionNameRequired
	}
	if registry == nil {
		return errors.New("hatriecache: nil collection metrics registry")
	}
	registry.mu.Lock()
	if registry.collections == nil {
		registry.collections = make(map[string]CollectionMetrics)
	}
	metrics := registry.collections[collection]
	metrics.Collection = collection
	metrics.Entries = entries
	metrics.Bytes = bytes
	registry.collections[collection] = metrics
	registry.mu.Unlock()
	return nil
}

// RecordCompaction increments the completed compaction counter for collection.
// It creates a zero-sized collection entry when no size has been reported yet.
func (registry *CollectionMetricsRegistry) RecordCompaction(collection string) error {
	collection = strings.TrimSpace(collection)
	if collection == "" {
		return ErrCollectionNameRequired
	}
	if registry == nil {
		return errors.New("hatriecache: nil collection metrics registry")
	}
	registry.mu.Lock()
	if registry.collections == nil {
		registry.collections = make(map[string]CollectionMetrics)
	}
	metrics := registry.collections[collection]
	metrics.Collection = collection
	metrics.CompactionsTotal++
	registry.collections[collection] = metrics
	registry.mu.Unlock()
	return nil
}

// Snapshot returns independently owned collection metrics sorted by collection
// name for deterministic export and comparison.
func (registry *CollectionMetricsRegistry) Snapshot() []CollectionMetrics {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	collections := make([]string, 0, len(registry.collections))
	for collection := range registry.collections {
		collections = append(collections, collection)
	}
	sort.Strings(collections)
	rows := make([]CollectionMetrics, 0, len(collections))
	for _, collection := range collections {
		rows = append(rows, registry.collections[collection])
	}
	registry.mu.RUnlock()
	return rows
}
