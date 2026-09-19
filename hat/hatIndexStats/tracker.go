// Package hatIndexStats provides bounded, opt-in diagnostics for indexes.
package hatIndexStats

import (
	"bytes"
	"errors"
	"math"
	"math/bits"
	"sort"
	"sync"
)

const (
	defaultTopK         = 8
	maxTopK             = 64
	defaultSampleEvery  = 16
	maxSampleEvery      = 1 << 20
	defaultMaxKeyBytes  = 128
	maxEncodedKeyBytes  = 1 << 16
	hllRegisterCount    = 64
	hllRegisterMask     = hllRegisterCount - 1
	hllSmallRangeFactor = 2.5
)

var ErrInvalidConfig = errors.New("invalid index statistics configuration")

// Config bounds diagnostic work. SampleEvery controls how often hot-key and
// posting-total samples are recorded; exact operation and hit counters remain
// available for every observation.
type Config struct {
	TopK        int
	SampleEvery uint64
	MaxKeyBytes int
}

// DefaultConfig enables bounded, low-frequency diagnostics.
func DefaultConfig() Config {
	return Config{
		TopK:        defaultTopK,
		SampleEvery: defaultSampleEvery,
		MaxKeyBytes: defaultMaxKeyBytes,
	}
}

// Tracker collects approximate cardinality and sampled hot-key diagnostics.
// It is safe for concurrent use and does not allocate per Observe call.
type Tracker struct {
	mu            sync.Mutex
	topK          int
	sampleEvery   uint64
	maxKeyBytes   int
	operations    uint64
	hits          uint64
	misses        uint64
	sampled       uint64
	postingTotal  uint64
	maxPostingLen uint64
	registers     [hllRegisterCount]uint8
	top           []hotKey
}

type hotKey struct {
	hash              uint64
	estimatedCount    uint64
	lastPostingLength uint64
	key               []byte
	hasKey            bool
}

// HotKey is one sampled heavy-hitter entry. A false HasKey means the entry is
// represented by Hash only because the original key exceeded MaxKeyBytes.
type HotKey struct {
	Hash              uint64
	Key               []byte
	HasKey            bool
	EstimatedCount    uint64
	LastPostingLength uint64
}

// Snapshot is a consistent diagnostic view. ApproxDistinct and sampled
// posting totals are estimates; operation and hit/miss counters are exact.
type Snapshot struct {
	Operations          uint64
	Hits                uint64
	Misses              uint64
	SampledOperations   uint64
	SampledPostingTotal uint64
	MaxPostingLength    uint64
	ApproxDistinct      uint64
	TopKeys             []HotKey
}

// New creates a tracker with bounded diagnostic state. Zero-valued limits use
// DefaultConfig values.
func New(config Config) (*Tracker, error) {
	defaults := DefaultConfig()
	if config.TopK == 0 {
		config.TopK = defaults.TopK
	}
	if config.SampleEvery == 0 {
		config.SampleEvery = defaults.SampleEvery
	}
	if config.MaxKeyBytes == 0 {
		config.MaxKeyBytes = defaults.MaxKeyBytes
	}
	if config.TopK < 1 || config.TopK > maxTopK || config.SampleEvery > maxSampleEvery || config.MaxKeyBytes < 1 || config.MaxKeyBytes > maxEncodedKeyBytes {
		return nil, ErrInvalidConfig
	}
	return &Tracker{
		topK:        config.TopK,
		sampleEvery: config.SampleEvery,
		maxKeyBytes: config.MaxKeyBytes,
		top:         make([]hotKey, 0, config.TopK),
	}, nil
}

// Observe records one index lookup and its posting-list length. The key is
// hashed immediately; callers may reuse the input slice after return.
func (t *Tracker) Observe(key []byte, postingLength uint64, hit bool) {
	if t == nil {
		return
	}
	hash := hashKey(key)
	t.mu.Lock()
	t.operations = saturatingAdd(t.operations, 1)
	if hit {
		t.hits = saturatingAdd(t.hits, 1)
	} else {
		t.misses = saturatingAdd(t.misses, 1)
	}
	if postingLength > t.maxPostingLen {
		t.maxPostingLen = postingLength
	}
	t.updateCardinalityLocked(hash)
	if t.operations%t.sampleEvery == 0 {
		t.sampled = saturatingAdd(t.sampled, 1)
		t.postingTotal = saturatingAdd(t.postingTotal, postingLength)
		t.updateHotKeyLocked(hash, key, postingLength)
	}
	t.mu.Unlock()
}

// Snapshot returns a sorted, isolated diagnostic view. TopKeys are ordered by
// estimated count descending and hash ascending for deterministic ties.
func (t *Tracker) Snapshot() Snapshot {
	if t == nil {
		return Snapshot{}
	}
	t.mu.Lock()
	snapshot := Snapshot{
		Operations:          t.operations,
		Hits:                t.hits,
		Misses:              t.misses,
		SampledOperations:   t.sampled,
		SampledPostingTotal: t.postingTotal,
		MaxPostingLength:    t.maxPostingLen,
		ApproxDistinct:      estimateDistinct(t.registers),
		TopKeys:             make([]HotKey, len(t.top)),
	}
	for i, entry := range t.top {
		snapshot.TopKeys[i] = HotKey{
			Hash:              entry.hash,
			Key:               append([]byte(nil), entry.key...),
			HasKey:            entry.hasKey,
			EstimatedCount:    entry.estimatedCount,
			LastPostingLength: entry.lastPostingLength,
		}
	}
	t.mu.Unlock()
	sort.Slice(snapshot.TopKeys, func(i, j int) bool {
		if snapshot.TopKeys[i].EstimatedCount != snapshot.TopKeys[j].EstimatedCount {
			return snapshot.TopKeys[i].EstimatedCount > snapshot.TopKeys[j].EstimatedCount
		}
		return snapshot.TopKeys[i].Hash < snapshot.TopKeys[j].Hash
	})
	return snapshot
}

// Reset clears all counters and sampled state while retaining the configured
// bounds and backing storage.
func (t *Tracker) Reset() {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.operations = 0
	t.hits = 0
	t.misses = 0
	t.sampled = 0
	t.postingTotal = 0
	t.maxPostingLen = 0
	for i := range t.registers {
		t.registers[i] = 0
	}
	for i := range t.top {
		t.top[i] = hotKey{}
	}
	t.top = t.top[:0]
	t.mu.Unlock()
}

func (t *Tracker) updateCardinalityLocked(hash uint64) {
	index := hash >> 58
	remaining := hash << 6
	rank := bits.LeadingZeros64(remaining) + 1
	if rank > 64 {
		rank = 64
	}
	if uint8(rank) > t.registers[index&hllRegisterMask] {
		t.registers[index&hllRegisterMask] = uint8(rank)
	}
}

func (t *Tracker) updateHotKeyLocked(hash uint64, key []byte, postingLength uint64) {
	canStoreKey := len(key) <= t.maxKeyBytes
	for i := range t.top {
		entry := &t.top[i]
		if entry.hash != hash || entry.hasKey != canStoreKey || canStoreKey && !bytes.Equal(entry.key, key) {
			continue
		}
		entry.estimatedCount = saturatingAdd(entry.estimatedCount, t.sampleEvery)
		entry.lastPostingLength = postingLength
		return
	}
	weight := t.sampleEvery
	entry := hotKey{
		hash:              hash,
		estimatedCount:    weight,
		lastPostingLength: postingLength,
		hasKey:            canStoreKey,
	}
	if canStoreKey {
		entry.key = append([]byte(nil), key...)
	}
	if len(t.top) < t.topK {
		t.top = append(t.top, entry)
		return
	}
	minimum := 0
	for i := 1; i < len(t.top); i++ {
		if t.top[i].estimatedCount < t.top[minimum].estimatedCount || t.top[i].estimatedCount == t.top[minimum].estimatedCount && t.top[i].hash > t.top[minimum].hash {
			minimum = i
		}
	}
	entry.estimatedCount = saturatingAdd(t.top[minimum].estimatedCount, weight)
	t.top[minimum] = entry
}

func estimateDistinct(registers [hllRegisterCount]uint8) uint64 {
	var sum float64
	zeros := 0
	for _, register := range registers {
		if register == 0 {
			zeros++
		}
		sum += math.Ldexp(1, -int(register))
	}
	const alpha = 0.709
	estimate := alpha * hllRegisterCount * hllRegisterCount / sum
	if estimate <= hllSmallRangeFactor*hllRegisterCount && zeros > 0 {
		estimate = hllRegisterCount * math.Log(float64(hllRegisterCount)/float64(zeros))
	}
	if estimate >= float64(^uint64(0)) {
		return ^uint64(0)
	}
	return uint64(estimate + 0.5)
}

func hashKey(key []byte) uint64 {
	const (
		offset = uint64(14695981039346656037)
		prime  = uint64(1099511628211)
	)
	hash := offset
	for _, value := range key {
		hash ^= uint64(value)
		hash *= prime
	}
	return hash
}

func saturatingAdd(left, right uint64) uint64 {
	if right > ^uint64(0)-left {
		return ^uint64(0)
	}
	return left + right
}
