package hatStorage

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"
)

const (
	// MaxSpaceMemoryQuotaNameBytes bounds the name retained by a quota registry.
	MaxSpaceMemoryQuotaNameBytes = 256
)

var (
	// ErrSpaceMemoryQuotaInvalid indicates a nil quota or malformed quota name.
	ErrSpaceMemoryQuotaInvalid = errors.New("hatStorage: space memory quota is invalid")
	// ErrSpaceMemoryQuotaExceeded indicates that a bounded quota cannot admit the
	// requested bytes.
	ErrSpaceMemoryQuotaExceeded = errors.New("hatStorage: space memory quota exceeded")
	// ErrSpaceMemoryQuotaUnderflow indicates that more bytes were released than
	// the quota currently accounts for.
	ErrSpaceMemoryQuotaUnderflow = errors.New("hatStorage: space memory quota release underflow")
	// ErrSpaceMemoryQuotaOverflow indicates that the accounting counter would
	// overflow uint64.
	ErrSpaceMemoryQuotaOverflow = errors.New("hatStorage: space memory quota accounting overflow")
	// ErrSpaceMemoryQuotaDuplicate indicates that a registry already owns the
	// requested name.
	ErrSpaceMemoryQuotaDuplicate = errors.New("hatStorage: space memory quota name is already registered")
	// ErrSpaceMemoryQuotaNotFound indicates that a registry lookup found no name.
	ErrSpaceMemoryQuotaNotFound = errors.New("hatStorage: space memory quota name is not registered")
)

// SpaceMemoryQuota is an opt-in byte admission counter for one named space.
// A zero limit means unlimited admission but still keeps overflow-safe usage
// accounting. The name and limit are immutable so Reserve and Release need no
// mutex; configuration changes should build a new registry.
type SpaceMemoryQuota struct {
	name  string
	limit uint64
	used  atomic.Uint64
}

// SpaceMemoryQuotaSnapshot is an owned point-in-time quota report.
type SpaceMemoryQuotaSnapshot struct {
	Name           string `json:"name"`
	LimitBytes     uint64 `json:"limit_bytes"`
	UsedBytes      uint64 `json:"used_bytes"`
	AvailableBytes uint64 `json:"available_bytes"`
}

// SpaceMemoryQuotaRegistry maps named spaces to immutable quota handles. The
// registry is safe for concurrent registration, lookup, reporting, and
// reserve/release convenience calls. Hot paths can call Lookup once and retain
// the returned handle to avoid the registry lock on every operation.
type SpaceMemoryQuotaRegistry struct {
	mu     sync.RWMutex
	quotas map[string]*SpaceMemoryQuota
}

// NewSpaceMemoryQuota creates an opt-in quota handle. A zero limit is
// unlimited; a non-zero limit is the maximum admitted bytes.
func NewSpaceMemoryQuota(name string, limit uint64) (*SpaceMemoryQuota, error) {
	name, err := normalizeSpaceMemoryQuotaName(name)
	if err != nil {
		return nil, err
	}
	return &SpaceMemoryQuota{name: name, limit: limit}, nil
}

// Name returns the stable logical space name.
func (quota *SpaceMemoryQuota) Name() string {
	if quota == nil {
		return ""
	}
	return quota.name
}

// Limit returns the maximum admitted bytes. Zero means unlimited.
func (quota *SpaceMemoryQuota) Limit() uint64 {
	if quota == nil {
		return 0
	}
	return quota.limit
}

// Used returns the currently admitted byte count.
func (quota *SpaceMemoryQuota) Used() uint64 {
	if quota == nil {
		return 0
	}
	return quota.used.Load()
}

// Available returns bytes that can be admitted without exceeding the limit.
// For an unlimited quota it returns the remaining uint64 accounting range.
func (quota *SpaceMemoryQuota) Available() uint64 {
	if quota == nil {
		return 0
	}
	return quota.available(quota.used.Load())
}

func (quota *SpaceMemoryQuota) available(used uint64) uint64 {
	limit := quota.limit
	if limit == 0 {
		return ^uint64(0) - used
	}
	if used >= limit {
		return 0
	}
	return limit - used
}

// Reserve atomically admits bytes or returns a quota/overflow error. Callers
// should reserve before allocating and release after the allocation is freed.
func (quota *SpaceMemoryQuota) Reserve(bytes uint64) error {
	if quota == nil {
		return ErrSpaceMemoryQuotaInvalid
	}
	if bytes == 0 {
		return nil
	}
	for {
		used := quota.used.Load()
		if bytes > ^uint64(0)-used {
			return ErrSpaceMemoryQuotaOverflow
		}
		updated := used + bytes
		if quota.limit != 0 && updated > quota.limit {
			return ErrSpaceMemoryQuotaExceeded
		}
		if quota.used.CompareAndSwap(used, updated) {
			return nil
		}
	}
}

// Release atomically returns bytes to the quota. It rejects underflow so an
// accounting bug cannot silently make future allocations unlimited.
func (quota *SpaceMemoryQuota) Release(bytes uint64) error {
	if quota == nil {
		return ErrSpaceMemoryQuotaInvalid
	}
	if bytes == 0 {
		return nil
	}
	for {
		used := quota.used.Load()
		if bytes > used {
			return ErrSpaceMemoryQuotaUnderflow
		}
		if quota.used.CompareAndSwap(used, used-bytes) {
			return nil
		}
	}
}

// Snapshot returns the current quota state without changing accounting.
func (quota *SpaceMemoryQuota) Snapshot() SpaceMemoryQuotaSnapshot {
	if quota == nil {
		return SpaceMemoryQuotaSnapshot{}
	}
	used := quota.used.Load()
	return SpaceMemoryQuotaSnapshot{
		Name:           quota.name,
		LimitBytes:     quota.limit,
		UsedBytes:      used,
		AvailableBytes: quota.available(used),
	}
}

// NewSpaceMemoryQuotaRegistry creates an empty named quota registry.
func NewSpaceMemoryQuotaRegistry() *SpaceMemoryQuotaRegistry {
	return &SpaceMemoryQuotaRegistry{quotas: make(map[string]*SpaceMemoryQuota)}
}

// Register creates and registers one named quota. Names are trimmed before
// storage, and duplicate names are rejected.
func (registry *SpaceMemoryQuotaRegistry) Register(name string, limit uint64) (*SpaceMemoryQuota, error) {
	name, err := normalizeSpaceMemoryQuotaName(name)
	if err != nil {
		return nil, err
	}
	if registry == nil {
		return nil, ErrSpaceMemoryQuotaInvalid
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.quotas == nil {
		registry.quotas = make(map[string]*SpaceMemoryQuota)
	}
	if _, exists := registry.quotas[name]; exists {
		return nil, fmt.Errorf("%w: %s", ErrSpaceMemoryQuotaDuplicate, name)
	}
	quota := &SpaceMemoryQuota{name: name, limit: limit}
	registry.quotas[name] = quota
	return quota, nil
}

// Lookup returns the immutable handle for name. It trims the lookup name using
// the same rules as Register.
func (registry *SpaceMemoryQuotaRegistry) Lookup(name string) (*SpaceMemoryQuota, bool) {
	if registry == nil {
		return nil, false
	}
	name = strings.TrimSpace(name)
	registry.mu.RLock()
	quota, ok := registry.quotas[name]
	registry.mu.RUnlock()
	return quota, ok
}

// Reserve looks up name and admits bytes. Prefer Lookup plus the returned
// handle when the same space performs many operations.
func (registry *SpaceMemoryQuotaRegistry) Reserve(name string, bytes uint64) error {
	quota, ok := registry.Lookup(name)
	if !ok {
		return ErrSpaceMemoryQuotaNotFound
	}
	return quota.Reserve(bytes)
}

// Release looks up name and returns bytes to its accounting counter.
func (registry *SpaceMemoryQuotaRegistry) Release(name string, bytes uint64) error {
	quota, ok := registry.Lookup(name)
	if !ok {
		return ErrSpaceMemoryQuotaNotFound
	}
	return quota.Release(bytes)
}

// Snapshot returns independent reports sorted by name for deterministic export.
func (registry *SpaceMemoryQuotaRegistry) Snapshot() []SpaceMemoryQuotaSnapshot {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	names := make([]string, 0, len(registry.quotas))
	for name := range registry.quotas {
		names = append(names, name)
	}
	sort.Strings(names)
	rows := make([]SpaceMemoryQuotaSnapshot, 0, len(names))
	for _, name := range names {
		rows = append(rows, registry.quotas[name].Snapshot())
	}
	registry.mu.RUnlock()
	return rows
}

func normalizeSpaceMemoryQuotaName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > MaxSpaceMemoryQuotaNameBytes || !utf8.ValidString(name) {
		return "", ErrSpaceMemoryQuotaInvalid
	}
	return name, nil
}
