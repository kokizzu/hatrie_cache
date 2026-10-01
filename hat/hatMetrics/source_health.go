package hatMetrics

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultSourceHealthCapacity bounds source names when callers do not select
	// a capacity explicitly.
	DefaultSourceHealthCapacity = 1024
	// MaxSourceHealthCapacity prevents an untrusted source registry from
	// retaining an unbounded number of diagnostic records.
	MaxSourceHealthCapacity = 1 << 20
)

var (
	ErrSourceHealthCapacity      = errors.New("hatriecache: source health capacity exceeded")
	ErrSourceHealthStatusInvalid = errors.New("hatriecache: invalid source health status")
)

// SourceHealthStatus describes the latest observed condition of one source.
type SourceHealthStatus string

const (
	SourceHealthUnknown  SourceHealthStatus = "unknown"
	SourceHealthHealthy  SourceHealthStatus = "healthy"
	SourceHealthDegraded SourceHealthStatus = "degraded"
	SourceHealthFailed   SourceHealthStatus = "failed"
)

// SourceHealth is a bounded diagnostic record for one independent source.
// LastError is intentionally caller-supplied text and is capped by Record.
type SourceHealth struct {
	Source              string             `json:"source"`
	Status              SourceHealthStatus `json:"status"`
	Frontier            uint64             `json:"frontier"`
	Observed            uint64             `json:"observed"`
	Lag                 uint64             `json:"lag"`
	ConsecutiveFailures uint64             `json:"consecutive_failures"`
	LastError           string             `json:"last_error,omitempty"`
	UpdatedAtUnixNano   int64              `json:"updated_at_unix_nano"`
}

// SourceHealthRegistry retains one bounded health record per source.
// A nil registry or nil monitoring option keeps the default write path free of
// health bookkeeping.
type SourceHealthRegistry struct {
	mu         sync.RWMutex
	maxSources int
	states     map[string]SourceHealth
}

// NewSourceHealthRegistry creates a registry with a bounded source capacity.
// Non-positive capacity selects DefaultSourceHealthCapacity.
func NewSourceHealthRegistry(capacity int) *SourceHealthRegistry {
	if capacity <= 0 {
		capacity = DefaultSourceHealthCapacity
	}
	if capacity > MaxSourceHealthCapacity {
		capacity = MaxSourceHealthCapacity
	}
	return &SourceHealthRegistry{maxSources: capacity, states: make(map[string]SourceHealth, capacity)}
}

func validSourceHealthStatus(status SourceHealthStatus) bool {
	switch status {
	case SourceHealthUnknown, SourceHealthHealthy, SourceHealthDegraded, SourceHealthFailed:
		return true
	default:
		return false
	}
}

// Record updates one source's health and frontier atomically. Frontier values
// are monotone; an out-of-order health update cannot move progress backward.
// Degraded and failed records increment ConsecutiveFailures, healthy records
// clear the failure streak and error text, and unknown records leave the
// streak unchanged.
func (registry *SourceHealthRegistry) Record(source string, status SourceHealthStatus, frontier uint64, lastError string) error {
	source = strings.TrimSpace(source)
	if source == "" {
		return ErrSourceNameRequired
	}
	if !validSourceHealthStatus(status) {
		return fmt.Errorf("%w: %q", ErrSourceHealthStatusInvalid, status)
	}
	if registry == nil {
		return errors.New("hatriecache: nil source health registry")
	}
	lastError = strings.TrimSpace(lastError)
	if len(lastError) > 1024 {
		lastError = lastError[:1024]
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.states == nil {
		registry.states = make(map[string]SourceHealth)
	}
	state, ok := registry.states[source]
	if !ok {
		if registry.maxSources <= 0 {
			registry.maxSources = DefaultSourceHealthCapacity
		}
		if len(registry.states) >= registry.maxSources {
			return fmt.Errorf("%w: maximum=%d", ErrSourceHealthCapacity, registry.maxSources)
		}
		state.Source = source
	}
	state.Status = status
	if frontier > state.Frontier {
		state.Frontier = frontier
	}
	if status == SourceHealthHealthy {
		state.ConsecutiveFailures = 0
		state.LastError = ""
	} else if status == SourceHealthDegraded || status == SourceHealthFailed {
		if state.ConsecutiveFailures < ^uint64(0) {
			state.ConsecutiveFailures++
		}
		state.LastError = lastError
	} else {
		state.LastError = lastError
	}
	state.UpdatedAtUnixNano = time.Now().UnixNano()
	registry.states[source] = state
	return nil
}

// Snapshot returns independently owned, source-name-sorted records. Lag is
// zero when observed is older than a source frontier.
func (registry *SourceHealthRegistry) Snapshot(observed uint64) []SourceHealth {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	sources := make([]string, 0, len(registry.states))
	for source := range registry.states {
		sources = append(sources, source)
	}
	sort.Strings(sources)
	rows := make([]SourceHealth, 0, len(sources))
	for _, source := range sources {
		row := registry.states[source]
		row.Observed = observed
		if observed > row.Frontier {
			row.Lag = observed - row.Frontier
		}
		rows = append(rows, row)
	}
	registry.mu.RUnlock()
	return rows
}
