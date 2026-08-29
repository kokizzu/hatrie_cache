package hatSql

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

type CacheWarmKind string

const (
	CacheWarmIndex CacheWarmKind = "index"
	CacheWarmView  CacheWarmKind = "view"
)

// CacheWarmTarget identifies one index or materialized view that can be warmed.
type CacheWarmTarget struct {
	Kind CacheWarmKind
	Name string
}

// CacheWarmOptions bounds warm metadata and controls the hotness threshold.
type CacheWarmOptions struct {
	Threshold int
	Capacity  int
}

type cacheWarmEntry struct {
	hits    int
	warmed  bool
	warming bool
	warm    func(context.Context) error
}

// CacheWarmer invokes registered warm callbacks automatically after repeated
// accesses. Failed callbacks remain eligible for a later retry.
type CacheWarmer struct {
	mu      sync.Mutex
	options CacheWarmOptions
	entries map[CacheWarmTarget]*cacheWarmEntry
}

func NewCacheWarmer(options CacheWarmOptions) *CacheWarmer {
	if options.Threshold <= 0 {
		options.Threshold = 10
	}
	if options.Capacity <= 0 {
		options.Capacity = 128
	}
	return &CacheWarmer{options: options, entries: make(map[CacheWarmTarget]*cacheWarmEntry)}
}

// Register associates a target with its warm operation.
func (warmer *CacheWarmer) Register(target CacheWarmTarget, warm func(context.Context) error) error {
	if warmer == nil {
		return fmt.Errorf("cache warmer is nil")
	}
	target, err := normalizeCacheWarmTarget(target)
	if err != nil {
		return err
	}
	if warm == nil {
		return fmt.Errorf("cache warm callback is required")
	}
	warmer.mu.Lock()
	defer warmer.mu.Unlock()
	if _, exists := warmer.entries[target]; !exists && len(warmer.entries) >= warmer.options.Capacity {
		return fmt.Errorf("cache warmer capacity %d reached", warmer.options.Capacity)
	}
	warmer.entries[target] = &cacheWarmEntry{warm: warm}
	return nil
}

// Observe records one target access. warmed reports whether this call invoked
// the callback; a non-nil error means that invocation can be retried later.
func (warmer *CacheWarmer) Observe(ctx context.Context, target CacheWarmTarget) (warmed bool, err error) {
	if warmer == nil {
		return false, nil
	}
	target, err = normalizeCacheWarmTarget(target)
	if err != nil {
		return false, err
	}
	warmer.mu.Lock()
	entry := warmer.entries[target]
	if entry == nil || entry.warmed || entry.warming {
		warmer.mu.Unlock()
		return false, nil
	}
	entry.hits++
	if entry.hits < warmer.options.Threshold {
		warmer.mu.Unlock()
		return false, nil
	}
	entry.warming = true
	warm := entry.warm
	warmer.mu.Unlock()

	err = warm(ctx)
	warmer.mu.Lock()
	if current := warmer.entries[target]; current == entry {
		entry.warming = false
		if err == nil {
			entry.warmed = true
		}
	}
	warmer.mu.Unlock()
	return true, err
}

// Invalidate makes a target eligible to warm again after its backing data
// changes. It is intentionally a no-op for an unknown target.
func (warmer *CacheWarmer) Invalidate(target CacheWarmTarget) {
	if warmer == nil {
		return
	}
	target, err := normalizeCacheWarmTarget(target)
	if err != nil {
		return
	}
	warmer.mu.Lock()
	if entry := warmer.entries[target]; entry != nil {
		entry.hits, entry.warmed = 0, false
	}
	warmer.mu.Unlock()
}

func normalizeCacheWarmTarget(target CacheWarmTarget) (CacheWarmTarget, error) {
	target.Name = strings.TrimSpace(target.Name)
	if target.Name == "" {
		return CacheWarmTarget{}, fmt.Errorf("cache warm target name cannot be empty")
	}
	if target.Kind != CacheWarmIndex && target.Kind != CacheWarmView {
		return CacheWarmTarget{}, fmt.Errorf("unsupported cache warm target kind %q", target.Kind)
	}
	return target, nil
}
