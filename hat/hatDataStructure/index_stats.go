package hatDataStructure

import (
	"encoding/binary"
	"errors"
	"sort"
	"sync"
)

var (
	// ErrIndexStatsHotKeyCapacityInvalid indicates an unsupported hot-key bound.
	ErrIndexStatsHotKeyCapacityInvalid = errors.New("hatDataStructure: index stats hot-key capacity is invalid")
	// ErrIndexStatsPrecisionInvalid indicates an unsupported cardinality precision.
	ErrIndexStatsPrecisionInvalid = errors.New("hatDataStructure: index stats precision is invalid")
)

const (
	// DefaultIndexStatsHotKeyCapacity bounds the default heavy-hitter table.
	DefaultIndexStatsHotKeyCapacity = 16
	// MaxIndexStatsHotKeyCapacity prevents accidental unbounded diagnostic state.
	MaxIndexStatsHotKeyCapacity = 1024
	// DefaultIndexStatsHLLPrecision keeps cardinality state compact.
	DefaultIndexStatsHLLPrecision uint8 = 10
)

// IndexStatsOptions configures an opt-in index diagnostic collector. A zero
// field uses its bounded default.
type IndexStatsOptions struct {
	HotKeyCapacity int
	HLLPrecision   uint8
}

// IndexHotKeySnapshot is one approximate heavy-hitter entry. Error is the
// Space-Saving overestimate bound caused by an earlier evicted key.
type IndexHotKeySnapshot struct {
	KeyHash          uint64 `json:"key_hash"`
	Observations     uint64 `json:"observations"`
	Error            uint64 `json:"error"`
	MaxPostingLength uint64 `json:"max_posting_length"`
}

// IndexStatsSnapshot is a point-in-time diagnostic report. EstimatedDistinctKeys
// is approximate; posting and observation counters are exact for accepted
// calls to this collector.
type IndexStatsSnapshot struct {
	CardinalityObservations uint64                `json:"cardinality_observations"`
	EstimatedDistinctKeys   uint64                `json:"estimated_distinct_keys"`
	LookupObservations      uint64                `json:"lookup_observations"`
	PostingLengthSamples    uint64                `json:"posting_length_samples"`
	TotalPostingLength      uint64                `json:"total_posting_length"`
	MaxPostingLength        uint64                `json:"max_posting_length"`
	HotKeys                 []IndexHotKeySnapshot `json:"hot_keys"`
}

type indexHotKey struct {
	keyHash          uint64
	observations     uint64
	error            uint64
	maxPostingLength uint64
}

// IndexStats collects bounded cardinality, posting-length, and hot-key
// diagnostics for an index. It does not modify or automatically instrument an
// index; callers opt in by observing index keys and lookups.
type IndexStats struct {
	mu sync.Mutex

	hll              HyperLogLog
	hotKeyCapacity   int
	hotKeys          []indexHotKey
	cardinalityCalls uint64
	lookupCalls      uint64
	postingSamples   uint64
	totalPosting     uint64
	maxPosting       uint64
}

// NewIndexStats creates an index diagnostic collector with bounded memory.
func NewIndexStats(options IndexStatsOptions) (*IndexStats, error) {
	hotKeyCapacity := options.HotKeyCapacity
	if hotKeyCapacity == 0 {
		hotKeyCapacity = DefaultIndexStatsHotKeyCapacity
	}
	if hotKeyCapacity < 1 || hotKeyCapacity > MaxIndexStatsHotKeyCapacity {
		return nil, ErrIndexStatsHotKeyCapacityInvalid
	}

	precision := options.HLLPrecision
	if precision == 0 {
		precision = DefaultIndexStatsHLLPrecision
	}
	hll, err := NewHyperLogLog(precision)
	if err != nil {
		return nil, ErrIndexStatsPrecisionInvalid
	}
	return &IndexStats{
		hll:            hll,
		hotKeyCapacity: hotKeyCapacity,
		hotKeys:        make([]indexHotKey, 0, hotKeyCapacity),
	}, nil
}

// NewDefaultIndexStats creates a collector with compact bounded defaults.
func NewDefaultIndexStats() *IndexStats {
	stats, err := NewIndexStats(IndexStatsOptions{})
	if err != nil {
		return nil
	}
	return stats
}

// ObserveKeyHash adds one index-key hash to the approximate cardinality
// estimator. Hashes should be stable and well-distributed; the original key
// is deliberately not retained.
func (stats *IndexStats) ObserveKeyHash(keyHash uint64) {
	if stats == nil {
		return
	}
	stats.mu.Lock()
	stats.ensureInitializedLocked()
	stats.cardinalityCalls++
	stats.observeHashLocked(keyHash)
	stats.mu.Unlock()
}

// ObserveLookup records one lookup and its posting-list length. The key hash
// also contributes to the distinct-key estimate and bounded hot-key table.
func (stats *IndexStats) ObserveLookup(keyHash uint64, postingLength uint64) {
	if stats == nil {
		return
	}
	stats.mu.Lock()
	stats.ensureInitializedLocked()
	stats.lookupCalls++
	stats.postingSamples++
	stats.totalPosting += postingLength
	if postingLength > stats.maxPosting {
		stats.maxPosting = postingLength
	}
	stats.observeHashLocked(keyHash)
	stats.observeHotKeyLocked(keyHash, postingLength)
	stats.mu.Unlock()
}

// Snapshot returns an owned report sorted by descending estimated hot-key
// frequency and then ascending key hash.
func (stats *IndexStats) Snapshot() IndexStatsSnapshot {
	if stats == nil {
		return IndexStatsSnapshot{}
	}
	stats.mu.Lock()
	defer stats.mu.Unlock()
	stats.ensureInitializedLocked()

	snapshot := IndexStatsSnapshot{
		CardinalityObservations: stats.cardinalityCalls,
		EstimatedDistinctKeys:   stats.hll.Count(),
		LookupObservations:      stats.lookupCalls,
		PostingLengthSamples:    stats.postingSamples,
		TotalPostingLength:      stats.totalPosting,
		MaxPostingLength:        stats.maxPosting,
	}
	if len(stats.hotKeys) == 0 {
		return snapshot
	}
	snapshot.HotKeys = make([]IndexHotKeySnapshot, len(stats.hotKeys))
	for i, hotKey := range stats.hotKeys {
		snapshot.HotKeys[i] = IndexHotKeySnapshot{
			KeyHash:          hotKey.keyHash,
			Observations:     hotKey.observations,
			Error:            hotKey.error,
			MaxPostingLength: hotKey.maxPostingLength,
		}
	}
	sort.Slice(snapshot.HotKeys, func(i, j int) bool {
		if snapshot.HotKeys[i].Observations != snapshot.HotKeys[j].Observations {
			return snapshot.HotKeys[i].Observations > snapshot.HotKeys[j].Observations
		}
		return snapshot.HotKeys[i].KeyHash < snapshot.HotKeys[j].KeyHash
	})
	return snapshot
}

func (stats *IndexStats) ensureInitializedLocked() {
	if stats.hll.Precision() == 0 {
		stats.hll = NewDefaultHyperLogLog()
	}
	if stats.hotKeyCapacity == 0 {
		stats.hotKeyCapacity = DefaultIndexStatsHotKeyCapacity
		stats.hotKeys = make([]indexHotKey, 0, stats.hotKeyCapacity)
	}
}

func (stats *IndexStats) observeHashLocked(keyHash uint64) {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], keyHash)
	stats.hll.AddBytes(encoded[:])
}

func (stats *IndexStats) observeHotKeyLocked(keyHash uint64, postingLength uint64) {
	for i := range stats.hotKeys {
		if stats.hotKeys[i].keyHash != keyHash {
			continue
		}
		stats.hotKeys[i].observations++
		if postingLength > stats.hotKeys[i].maxPostingLength {
			stats.hotKeys[i].maxPostingLength = postingLength
		}
		return
	}

	if len(stats.hotKeys) < stats.hotKeyCapacity {
		stats.hotKeys = append(stats.hotKeys, indexHotKey{
			keyHash:          keyHash,
			observations:     1,
			maxPostingLength: postingLength,
		})
		return
	}

	minimum := 0
	for i := 1; i < len(stats.hotKeys); i++ {
		if stats.hotKeys[i].observations < stats.hotKeys[minimum].observations {
			minimum = i
		}
	}
	previous := stats.hotKeys[minimum]
	stats.hotKeys[minimum] = indexHotKey{
		keyHash:          keyHash,
		observations:     previous.observations + 1,
		error:            previous.observations,
		maxPostingLength: postingLength,
	}
}
