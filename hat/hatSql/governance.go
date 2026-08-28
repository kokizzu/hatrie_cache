package hatSql

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// NamespaceResourceLimits caps resources available to queries in one namespace.
// Zero leaves a limit unset. Timeout is wall-clock time, which bounds CPU work
// performed by the cooperative SQL executor; MaxWorkers caps parallel operators.
type NamespaceResourceLimits struct {
	MaxConcurrentQueries int
	MaxRows              int
	MaxJoinWork          int
	MaxJoinBytes         int
	MaxResultBytes       int
	MaxWorkers           int
	MaxSortBytes         int
	MaxGroupBytes        int
	MaxSetBytes          int
	MaxSpillBytes        int
	MaxRecursionDepth    int
	Timeout              time.Duration
	SpillDirectory       string
}

// Apply tightens options to this namespace policy. Positive policies are upper
// bounds; a caller's stricter value remains intact. A zero MaxRows is the
// executor's safe default, so a policy larger than that default cannot loosen it.
func (limits NamespaceResourceLimits) Apply(options SQLQueryOptions) SQLQueryOptions {
	options.MaxRows = applyRowLimit(options.MaxRows, limits.MaxRows)
	options.MaxJoinWork = applyPositiveLimit(options.MaxJoinWork, limits.MaxJoinWork)
	options.MaxJoinBytes = applyPositiveLimit(options.MaxJoinBytes, limits.MaxJoinBytes)
	options.MaxResultBytes = applyPositiveLimit(options.MaxResultBytes, limits.MaxResultBytes)
	options.Workers = applyPositiveLimit(options.Workers, limits.MaxWorkers)
	options.MaxSortBytes = applyPositiveLimit(options.MaxSortBytes, limits.MaxSortBytes)
	options.MaxGroupBytes = applyPositiveLimit(options.MaxGroupBytes, limits.MaxGroupBytes)
	options.MaxSetBytes = applyPositiveLimit(options.MaxSetBytes, limits.MaxSetBytes)
	options.MaxSpillBytes = applyPositiveLimit(options.MaxSpillBytes, limits.MaxSpillBytes)
	options.MaxRecursionDepth = applyPositiveLimit(options.MaxRecursionDepth, limits.MaxRecursionDepth)
	options.Timeout = applyDurationLimit(options.Timeout, limits.Timeout)
	if limits.SpillDirectory != "" {
		options.SpillDirectory = limits.SpillDirectory
	}
	return options
}

func applyRowLimit(requested, maximum int) int {
	if maximum <= 0 {
		return requested
	}
	if requested == 0 {
		if maximum < maxSQLQueryRows {
			return maximum
		}
		return 0
	}
	return applyPositiveLimit(requested, maximum)
}

func applyPositiveLimit(requested, maximum int) int {
	if maximum > 0 && (requested == 0 || requested > maximum) {
		return maximum
	}
	return requested
}

func applyDurationLimit(requested, maximum time.Duration) time.Duration {
	if maximum > 0 && (requested == 0 || requested > maximum) {
		return maximum
	}
	return requested
}

func (limits NamespaceResourceLimits) validate() error {
	values := []struct {
		name  string
		value int
	}{
		{"max concurrent queries", limits.MaxConcurrentQueries},
		{"max rows", limits.MaxRows},
		{"max join work", limits.MaxJoinWork},
		{"max join bytes", limits.MaxJoinBytes},
		{"max result bytes", limits.MaxResultBytes},
		{"max workers", limits.MaxWorkers},
		{"max sort bytes", limits.MaxSortBytes},
		{"max group bytes", limits.MaxGroupBytes},
		{"max set bytes", limits.MaxSetBytes},
		{"max spill bytes", limits.MaxSpillBytes},
		{"max recursion depth", limits.MaxRecursionDepth},
	}
	for _, value := range values {
		if value.value < 0 {
			return fmt.Errorf("namespace resource limit %s must not be negative", value.name)
		}
	}
	if limits.Timeout < 0 {
		return fmt.Errorf("namespace resource limit timeout must not be negative")
	}
	return nil
}

// NamespaceQueryGovernor applies immutable default and per-namespace resource
// limits before delegating to the package's single SQL execution path.
type NamespaceQueryGovernor struct {
	defaults   NamespaceResourceLimits
	namespaces map[string]NamespaceResourceLimits

	mu    sync.Mutex
	gates map[string]*namespaceQueryGate
}

// NewNamespaceQueryGovernor validates and copies the supplied static policies.
// Per-namespace values only tighten defaults, which prevents accidental policy
// escalation in configuration overlays.
func NewNamespaceQueryGovernor(defaults NamespaceResourceLimits, namespaces map[string]NamespaceResourceLimits) (*NamespaceQueryGovernor, error) {
	if err := defaults.validate(); err != nil {
		return nil, err
	}
	copyNamespaces := make(map[string]NamespaceResourceLimits, len(namespaces))
	for namespace, limits := range namespaces {
		if strings.TrimSpace(namespace) == "" {
			return nil, fmt.Errorf("namespace resource policy name is required")
		}
		if err := limits.validate(); err != nil {
			return nil, fmt.Errorf("namespace %q: %w", namespace, err)
		}
		copyNamespaces[namespace] = tightenNamespaceLimits(defaults, limits)
	}
	return &NamespaceQueryGovernor{
		defaults:   defaults,
		namespaces: copyNamespaces,
		gates:      make(map[string]*namespaceQueryGate),
	}, nil
}

func tightenNamespaceLimits(defaults, override NamespaceResourceLimits) NamespaceResourceLimits {
	return NamespaceResourceLimits{
		MaxConcurrentQueries: applyPositiveLimit(defaults.MaxConcurrentQueries, override.MaxConcurrentQueries),
		MaxRows:              applyPositiveLimit(defaults.MaxRows, override.MaxRows),
		MaxJoinWork:          applyPositiveLimit(defaults.MaxJoinWork, override.MaxJoinWork),
		MaxJoinBytes:         applyPositiveLimit(defaults.MaxJoinBytes, override.MaxJoinBytes),
		MaxResultBytes:       applyPositiveLimit(defaults.MaxResultBytes, override.MaxResultBytes),
		MaxWorkers:           applyPositiveLimit(defaults.MaxWorkers, override.MaxWorkers),
		MaxSortBytes:         applyPositiveLimit(defaults.MaxSortBytes, override.MaxSortBytes),
		MaxGroupBytes:        applyPositiveLimit(defaults.MaxGroupBytes, override.MaxGroupBytes),
		MaxSetBytes:          applyPositiveLimit(defaults.MaxSetBytes, override.MaxSetBytes),
		MaxSpillBytes:        applyPositiveLimit(defaults.MaxSpillBytes, override.MaxSpillBytes),
		MaxRecursionDepth:    applyPositiveLimit(defaults.MaxRecursionDepth, override.MaxRecursionDepth),
		Timeout:              applyDurationLimit(defaults.Timeout, override.Timeout),
		SpillDirectory:       firstNonEmpty(defaults.SpillDirectory, override.SpillDirectory),
	}
}

func firstNonEmpty(first, second string) string {
	if first != "" {
		return first
	}
	return second
}

func (governor *NamespaceQueryGovernor) limitsFor(namespace string) NamespaceResourceLimits {
	if limits, ok := governor.namespaces[namespace]; ok {
		return limits
	}
	return governor.defaults
}

func (governor *NamespaceQueryGovernor) gateFor(namespace string, limits NamespaceResourceLimits) *namespaceQueryGate {
	if limits.MaxConcurrentQueries == 0 {
		return nil
	}
	governor.mu.Lock()
	defer governor.mu.Unlock()
	if gate := governor.gates[namespace]; gate != nil {
		return gate
	}
	gate := newNamespaceQueryGate(limits.MaxConcurrentQueries)
	governor.gates[namespace] = gate
	return gate
}

// Execute runs source under the named namespace's policy. Waiting for a busy
// namespace observes ctx cancellation and never starts a second SQL path.
func (governor *NamespaceQueryGovernor) Execute(ctx context.Context, namespace, source string, resolver SQLSourceResolver, parameters []interface{}, options SQLQueryOptions) (SQLQueryResult, error) {
	if governor == nil {
		return SQLQueryResult{}, fmt.Errorf("namespace query governor is required")
	}
	if strings.TrimSpace(namespace) == "" {
		return SQLQueryResult{}, fmt.Errorf("namespace is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	limits := governor.limitsFor(namespace)
	gate := governor.gateFor(namespace, limits)
	if gate != nil {
		if err := gate.acquire(ctx); err != nil {
			return SQLQueryResult{}, err
		}
		defer gate.release()
	}
	return ExecuteSQLQueryParameters(ctx, source, resolver, parameters, limits.Apply(options))
}

// namespaceQueryGate bounds one namespace while admitting waiters in arrival
// order. A canceled waiter is removed before it can consume a released slot.
type namespaceQueryGate struct {
	mu       sync.Mutex
	capacity int
	running  int
	waiters  []*namespaceQueryWaiter
}

type namespaceQueryWaiter struct {
	ready     chan struct{}
	granted   bool
	cancelled bool
}

func newNamespaceQueryGate(capacity int) *namespaceQueryGate {
	return &namespaceQueryGate{capacity: capacity}
}

func (gate *namespaceQueryGate) acquire(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	gate.mu.Lock()
	if gate.running < gate.capacity && len(gate.waiters) == 0 {
		gate.running++
		gate.mu.Unlock()
		return nil
	}
	waiter := &namespaceQueryWaiter{ready: make(chan struct{})}
	gate.waiters = append(gate.waiters, waiter)
	gate.mu.Unlock()

	select {
	case <-waiter.ready:
		if err := ctx.Err(); err != nil {
			gate.release()
			return err
		}
		return nil
	case <-ctx.Done():
		gate.mu.Lock()
		if !waiter.granted {
			waiter.cancelled = true
			gate.removeWaiterLocked(waiter)
			gate.mu.Unlock()
			return ctx.Err()
		}
		gate.mu.Unlock()
		gate.release()
		return ctx.Err()
	}
}

func (gate *namespaceQueryGate) release() {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	for len(gate.waiters) > 0 {
		waiter := gate.waiters[0]
		gate.waiters[0] = nil
		gate.waiters = gate.waiters[1:]
		if waiter.cancelled {
			continue
		}
		waiter.granted = true
		close(waiter.ready)
		return
	}
	gate.running--
}

func (gate *namespaceQueryGate) removeWaiterLocked(target *namespaceQueryWaiter) {
	for index, waiter := range gate.waiters {
		if waiter != target {
			continue
		}
		copy(gate.waiters[index:], gate.waiters[index+1:])
		gate.waiters[len(gate.waiters)-1] = nil
		gate.waiters = gate.waiters[:len(gate.waiters)-1]
		return
	}
}
