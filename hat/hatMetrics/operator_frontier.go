package hatMetrics

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

var ErrOperatorFrontierRegressed = errors.New("hatriecache: operator frontier regressed")

// OperatorFrontier is a point-in-time progress snapshot for one operator.
type OperatorFrontier struct {
	Operator string `json:"operator"`
	Frontier uint64 `json:"frontier"`
	Observed uint64 `json:"observed"`
	Lag      uint64 `json:"lag"`
}

// OperatorFrontierRegistry tracks monotone progress for independent
// operators. It is safe for concurrent reporters and snapshot readers.
type OperatorFrontierRegistry struct {
	mu        sync.RWMutex
	frontiers map[string]uint64
}

// NewOperatorFrontierRegistry creates an empty operator frontier registry.
func NewOperatorFrontierRegistry() *OperatorFrontierRegistry {
	return &OperatorFrontierRegistry{frontiers: make(map[string]uint64)}
}

// Advance records frontier when it is not older than the operator's current
// frontier. Equal updates are accepted and are idempotent.
func (registry *OperatorFrontierRegistry) Advance(operator string, frontier uint64) error {
	operator = strings.TrimSpace(operator)
	if operator == "" {
		return ErrOperatorNameRequired
	}
	if registry == nil {
		return errors.New("hatriecache: nil operator frontier registry")
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.frontiers == nil {
		registry.frontiers = make(map[string]uint64)
	}
	if current, ok := registry.frontiers[operator]; ok && frontier < current {
		return fmt.Errorf("%w for %q: current=%d requested=%d", ErrOperatorFrontierRegressed, operator, current, frontier)
	}
	registry.frontiers[operator] = frontier
	return nil
}

// Frontier returns the latest frontier for operator.
func (registry *OperatorFrontierRegistry) Frontier(operator string) (uint64, bool) {
	if registry == nil {
		return 0, false
	}
	operator = strings.TrimSpace(operator)
	if operator == "" {
		return 0, false
	}
	registry.mu.RLock()
	frontier, ok := registry.frontiers[operator]
	registry.mu.RUnlock()
	return frontier, ok
}

// Delete removes operator progress and reports whether it was present. This
// lets callers release state when an operator is dropped or rebuilt.
func (registry *OperatorFrontierRegistry) Delete(operator string) bool {
	if registry == nil {
		return false
	}
	operator = strings.TrimSpace(operator)
	if operator == "" {
		return false
	}
	registry.mu.Lock()
	_, ok := registry.frontiers[operator]
	delete(registry.frontiers, operator)
	registry.mu.Unlock()
	return ok
}

// Snapshot returns independently owned operator progress rows sorted by
// operator name. Lag is zero when observed is older than a frontier.
func (registry *OperatorFrontierRegistry) Snapshot(observed uint64) []OperatorFrontier {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	operators := make([]string, 0, len(registry.frontiers))
	for operator := range registry.frontiers {
		operators = append(operators, operator)
	}
	sort.Strings(operators)
	rows := make([]OperatorFrontier, 0, len(operators))
	for _, operator := range operators {
		frontier := registry.frontiers[operator]
		lag := uint64(0)
		if observed > frontier {
			lag = observed - frontier
		}
		rows = append(rows, OperatorFrontier{
			Operator: operator,
			Frontier: frontier,
			Observed: observed,
			Lag:      lag,
		})
	}
	registry.mu.RUnlock()
	return rows
}
