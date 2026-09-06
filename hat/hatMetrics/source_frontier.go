package hatMetrics

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

var (
	ErrSourceNameRequired      = errors.New("hatriecache: source name is required")
	ErrSourceFrontierRegressed = errors.New("hatriecache: source frontier regressed")
)

// SourceFrontier is a point-in-time progress snapshot for one source.
type SourceFrontier struct {
	Source   string `json:"source"`
	Frontier uint64 `json:"frontier"`
	Observed uint64 `json:"observed"`
	Lag      uint64 `json:"lag"`
}

// SourceFrontierRegistry tracks monotone progress for independent sources.
// It is useful when a consumer observes one global frontier but sources may
// advance at different rates.
type SourceFrontierRegistry struct {
	mu        sync.RWMutex
	frontiers map[string]uint64
}

// NewSourceFrontierRegistry creates an empty source frontier registry.
func NewSourceFrontierRegistry() *SourceFrontierRegistry {
	return &SourceFrontierRegistry{frontiers: make(map[string]uint64)}
}

// Advance records frontier when it is not older than the source's current
// frontier. Equal updates are accepted and are idempotent.
func (registry *SourceFrontierRegistry) Advance(source string, frontier uint64) error {
	source = strings.TrimSpace(source)
	if source == "" {
		return ErrSourceNameRequired
	}
	if registry == nil {
		return errors.New("hatriecache: nil source frontier registry")
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.frontiers == nil {
		registry.frontiers = make(map[string]uint64)
	}
	if current, ok := registry.frontiers[source]; ok && frontier < current {
		return fmt.Errorf("%w for %q: current=%d requested=%d", ErrSourceFrontierRegressed, source, current, frontier)
	}
	registry.frontiers[source] = frontier
	return nil
}

// Frontier returns the latest frontier for source.
func (registry *SourceFrontierRegistry) Frontier(source string) (uint64, bool) {
	if registry == nil {
		return 0, false
	}
	source = strings.TrimSpace(source)
	if source == "" {
		return 0, false
	}
	registry.mu.RLock()
	frontier, ok := registry.frontiers[source]
	registry.mu.RUnlock()
	return frontier, ok
}

// Snapshot returns independently owned, source-name-sorted progress rows.
// Lag is zero when observed is older than a source frontier.
func (registry *SourceFrontierRegistry) Snapshot(observed uint64) []SourceFrontier {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	sources := make([]string, 0, len(registry.frontiers))
	for source := range registry.frontiers {
		sources = append(sources, source)
	}
	sort.Strings(sources)
	rows := make([]SourceFrontier, 0, len(sources))
	for _, source := range sources {
		frontier := registry.frontiers[source]
		lag := uint64(0)
		if observed > frontier {
			lag = observed - frontier
		}
		rows = append(rows, SourceFrontier{Source: source, Frontier: frontier, Observed: observed, Lag: lag})
	}
	registry.mu.RUnlock()
	return rows
}
