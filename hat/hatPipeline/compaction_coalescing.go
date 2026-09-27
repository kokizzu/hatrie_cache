package hatPipeline

import (
	"errors"
	"sync"
)

var (
	// ErrSchedulerCoalescingUnavailable reports that coalesced submission was
	// requested from a scheduler created without the opt-in coalescer.
	ErrSchedulerCoalescingUnavailable = errors.New("hatPipeline: compaction coalescing is not enabled")
)

type frontierCompactionKey struct {
	frontierID string
	boundary   uint64
}

type frontierCompactionCoalescer struct {
	mu      sync.Mutex
	pending map[frontierCompactionKey]struct{}
}

func newFrontierCompactionCoalescer() *frontierCompactionCoalescer {
	return &frontierCompactionCoalescer{
		pending: make(map[frontierCompactionKey]struct{}),
	}
}

func (coalescer *frontierCompactionCoalescer) reserve(key frontierCompactionKey) bool {
	coalescer.mu.Lock()
	defer coalescer.mu.Unlock()
	if _, exists := coalescer.pending[key]; exists {
		return false
	}
	coalescer.pending[key] = struct{}{}
	return true
}

func (coalescer *frontierCompactionCoalescer) release(key frontierCompactionKey) {
	coalescer.mu.Lock()
	delete(coalescer.pending, key)
	coalescer.mu.Unlock()
}

func (coalescer *frontierCompactionCoalescer) clear() {
	coalescer.mu.Lock()
	clear(coalescer.pending)
	coalescer.mu.Unlock()
}
