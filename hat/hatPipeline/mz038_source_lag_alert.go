package hatPipeline

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

const (
	DefaultSourceLagAlertWarningLag         uint64 = 100
	DefaultSourceLagAlertCriticalLag        uint64 = 1000
	DefaultSourceLagAlertMaxSources                = 1024
	DefaultSourceLagAlertMaxSourceNameBytes        = 256
)

var (
	ErrSourceLagAlertPolicyInvalid   = errors.New("hatPipeline: source lag alert policy is invalid")
	ErrSourceLagAlertSourceRequired  = errors.New("hatPipeline: source lag alert source is required")
	ErrSourceLagAlertSourceLimit     = errors.New("hatPipeline: source lag alert source limit exceeded")
	ErrSourceLagAlertSourceNameLimit = errors.New("hatPipeline: source lag alert source name is too long")
	ErrSourceLagAlertStateInvalid    = errors.New("hatPipeline: source lag alert state is invalid")
	ErrSourceLagAlertSnapshotInvalid = errors.New("hatPipeline: source lag alert snapshot is invalid")
)

// SourceLagAlertState is the bounded alert state for one source.
type SourceLagAlertState string

const (
	SourceLagAlertHealthy  SourceLagAlertState = "healthy"
	SourceLagAlertWarning  SourceLagAlertState = "warning"
	SourceLagAlertCritical SourceLagAlertState = "critical"
)

// SourceLagAlertPolicy defines lag thresholds and recovery hysteresis. Zero
// fields select the documented defaults.
type SourceLagAlertPolicy struct {
	WarningLag  uint64 `json:"warning_lag"`
	CriticalLag uint64 `json:"critical_lag"`
	RecoveryLag uint64 `json:"recovery_lag"`
}

// DefaultSourceLagAlertPolicy returns bounded thresholds suitable for a
// source whose lag is measured in logical records or frontier units.
func DefaultSourceLagAlertPolicy() SourceLagAlertPolicy {
	return SourceLagAlertPolicy{
		WarningLag:  DefaultSourceLagAlertWarningLag,
		CriticalLag: DefaultSourceLagAlertCriticalLag,
		RecoveryLag: DefaultSourceLagAlertWarningLag / 2,
	}
}

func (policy SourceLagAlertPolicy) normalize() (SourceLagAlertPolicy, error) {
	defaults := DefaultSourceLagAlertPolicy()
	if policy.WarningLag == 0 {
		policy.WarningLag = defaults.WarningLag
	}
	if policy.CriticalLag == 0 {
		policy.CriticalLag = defaults.CriticalLag
	}
	if policy.RecoveryLag == 0 {
		policy.RecoveryLag = policy.WarningLag / 2
	}
	if policy.WarningLag == 0 || policy.CriticalLag <= policy.WarningLag || policy.RecoveryLag >= policy.WarningLag {
		return SourceLagAlertPolicy{}, fmt.Errorf("%w: warning=%d critical=%d recovery=%d", ErrSourceLagAlertPolicyInvalid, policy.WarningLag, policy.CriticalLag, policy.RecoveryLag)
	}
	return policy, nil
}

// SourceLagAlertRegistryOptions bounds one source-alert registry.
type SourceLagAlertRegistryOptions struct {
	Policy             SourceLagAlertPolicy
	MaxSources         int
	MaxSourceNameBytes int
}

// SourceLagAlert is the current bounded state for one source.
type SourceLagAlert struct {
	Source      string              `json:"source"`
	State       SourceLagAlertState `json:"state"`
	Lag         uint64              `json:"lag"`
	Transitions uint64              `json:"transitions"`
}

// SourceLagAlertTransition describes one observation and whether it changed
// the source's alert state.
type SourceLagAlertTransition struct {
	Source      string              `json:"source"`
	Previous    SourceLagAlertState `json:"previous"`
	Current     SourceLagAlertState `json:"current"`
	Lag         uint64              `json:"lag"`
	Changed     bool                `json:"changed"`
	Transitions uint64              `json:"transitions"`
}

// SourceLagAlertRegistrySnapshot is a deterministic, restart-safe state copy.
type SourceLagAlertRegistrySnapshot struct {
	Policy SourceLagAlertPolicy `json:"policy"`
	Alerts []SourceLagAlert     `json:"alerts"`
}

// SourceLagAlertRegistry tracks bounded per-source lag states. It is safe for
// concurrent observations, snapshots, and restores.
type SourceLagAlertRegistry struct {
	mu                 sync.RWMutex
	policy             SourceLagAlertPolicy
	maxSources         int
	maxSourceNameBytes int
	alerts             map[string]SourceLagAlert
}

// NewSourceLagAlertRegistry creates a bounded source lag registry.
func NewSourceLagAlertRegistry(options SourceLagAlertRegistryOptions) (*SourceLagAlertRegistry, error) {
	policy, err := options.Policy.normalize()
	if err != nil {
		return nil, err
	}
	maxSources := options.MaxSources
	if maxSources == 0 {
		maxSources = DefaultSourceLagAlertMaxSources
	}
	if maxSources < 1 {
		return nil, fmt.Errorf("%w: max sources=%d", ErrSourceLagAlertPolicyInvalid, maxSources)
	}
	maxNameBytes := options.MaxSourceNameBytes
	if maxNameBytes == 0 {
		maxNameBytes = DefaultSourceLagAlertMaxSourceNameBytes
	}
	if maxNameBytes < 1 {
		return nil, fmt.Errorf("%w: max source name bytes=%d", ErrSourceLagAlertPolicyInvalid, maxNameBytes)
	}
	return &SourceLagAlertRegistry{
		policy:             policy,
		maxSources:         maxSources,
		maxSourceNameBytes: maxNameBytes,
		alerts:             make(map[string]SourceLagAlert, maxSources),
	}, nil
}

func normalizeSourceLagAlertSource(source string, maxBytes int) (string, error) {
	source = strings.TrimSpace(source)
	if source == "" {
		return "", ErrSourceLagAlertSourceRequired
	}
	if len(source) > maxBytes {
		return "", fmt.Errorf("%w: %d > %d", ErrSourceLagAlertSourceNameLimit, len(source), maxBytes)
	}
	return source, nil
}

func validSourceLagAlertState(state SourceLagAlertState) bool {
	return state == SourceLagAlertHealthy || state == SourceLagAlertWarning || state == SourceLagAlertCritical
}

func nextSourceLagAlertState(previous SourceLagAlertState, lag uint64, policy SourceLagAlertPolicy) SourceLagAlertState {
	switch previous {
	case SourceLagAlertCritical:
		if lag >= policy.CriticalLag {
			return SourceLagAlertCritical
		}
		if lag <= policy.RecoveryLag {
			return SourceLagAlertHealthy
		}
		return SourceLagAlertWarning
	case SourceLagAlertWarning:
		if lag >= policy.CriticalLag {
			return SourceLagAlertCritical
		}
		if lag <= policy.RecoveryLag {
			return SourceLagAlertHealthy
		}
		return SourceLagAlertWarning
	default:
		if lag >= policy.CriticalLag {
			return SourceLagAlertCritical
		}
		if lag >= policy.WarningLag {
			return SourceLagAlertWarning
		}
		return SourceLagAlertHealthy
	}
}

// Observe records lag and applies threshold hysteresis for source.
func (registry *SourceLagAlertRegistry) Observe(source string, lag uint64) (SourceLagAlertTransition, error) {
	if registry == nil {
		return SourceLagAlertTransition{}, ErrSourceLagAlertSnapshotInvalid
	}
	normalized, err := normalizeSourceLagAlertSource(source, registry.maxSourceNameBytes)
	if err != nil {
		return SourceLagAlertTransition{}, err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	alert, exists := registry.alerts[normalized]
	if !exists {
		if len(registry.alerts) >= registry.maxSources {
			return SourceLagAlertTransition{}, fmt.Errorf("%w: max sources=%d", ErrSourceLagAlertSourceLimit, registry.maxSources)
		}
		alert = SourceLagAlert{Source: normalized, State: SourceLagAlertHealthy}
	}
	previous := alert.State
	current := nextSourceLagAlertState(previous, lag, registry.policy)
	changed := current != previous
	if changed {
		alert.Transitions++
	}
	alert.State = current
	alert.Lag = lag
	registry.alerts[normalized] = alert
	return SourceLagAlertTransition{
		Source:      normalized,
		Previous:    previous,
		Current:     current,
		Lag:         lag,
		Changed:     changed,
		Transitions: alert.Transitions,
	}, nil
}

// Snapshot returns sorted, independently owned current alerts.
func (registry *SourceLagAlertRegistry) Snapshot() []SourceLagAlert {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	alerts := sourceLagAlertSnapshotLocked(registry.alerts)
	registry.mu.RUnlock()
	return alerts
}

// SnapshotState returns policy and alert state suitable for durable storage.
func (registry *SourceLagAlertRegistry) SnapshotState() SourceLagAlertRegistrySnapshot {
	if registry == nil {
		return SourceLagAlertRegistrySnapshot{}
	}
	registry.mu.RLock()
	snapshot := SourceLagAlertRegistrySnapshot{
		Policy: registry.policy,
		Alerts: sourceLagAlertSnapshotLocked(registry.alerts),
	}
	registry.mu.RUnlock()
	return snapshot
}

func sourceLagAlertSnapshotLocked(alerts map[string]SourceLagAlert) []SourceLagAlert {
	rows := make([]SourceLagAlert, 0, len(alerts))
	for _, alert := range alerts {
		rows = append(rows, alert)
	}
	sort.Slice(rows, func(left, right int) bool { return rows[left].Source < rows[right].Source })
	return rows
}

// Restore atomically adopts a validated snapshot. It rejects impossible state
// combinations instead of silently publishing a false recovery status.
func (registry *SourceLagAlertRegistry) Restore(snapshot SourceLagAlertRegistrySnapshot) error {
	if registry == nil {
		return ErrSourceLagAlertSnapshotInvalid
	}
	policy, err := snapshot.Policy.normalize()
	if err != nil {
		return fmt.Errorf("%w: policy: %v", ErrSourceLagAlertSnapshotInvalid, err)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if policy != registry.policy {
		return fmt.Errorf("%w: policy does not match registry", ErrSourceLagAlertSnapshotInvalid)
	}
	if len(snapshot.Alerts) > registry.maxSources {
		return fmt.Errorf("%w: %d alerts exceed %d sources", ErrSourceLagAlertSnapshotInvalid, len(snapshot.Alerts), registry.maxSources)
	}
	alerts := make(map[string]SourceLagAlert, len(snapshot.Alerts))
	for _, alert := range snapshot.Alerts {
		source, sourceErr := normalizeSourceLagAlertSource(alert.Source, registry.maxSourceNameBytes)
		if sourceErr != nil {
			return fmt.Errorf("%w: source: %v", ErrSourceLagAlertSnapshotInvalid, sourceErr)
		}
		if source != alert.Source || !validSourceLagAlertState(alert.State) {
			return fmt.Errorf("%w: invalid alert %q", ErrSourceLagAlertSnapshotInvalid, alert.Source)
		}
		if _, exists := alerts[source]; exists {
			return fmt.Errorf("%w: duplicate source %q", ErrSourceLagAlertSnapshotInvalid, source)
		}
		if alert.State == SourceLagAlertCritical && alert.Lag < policy.CriticalLag {
			return fmt.Errorf("%w: critical source %q has lag %d below %d", ErrSourceLagAlertSnapshotInvalid, source, alert.Lag, policy.CriticalLag)
		}
		if alert.State == SourceLagAlertHealthy && alert.Lag >= policy.WarningLag {
			return fmt.Errorf("%w: healthy source %q has lag %d at or above %d", ErrSourceLagAlertSnapshotInvalid, source, alert.Lag, policy.WarningLag)
		}
		alerts[source] = alert
	}
	registry.alerts = alerts
	return nil
}

// Delete removes a source and releases its bounded state.
func (registry *SourceLagAlertRegistry) Delete(source string) bool {
	if registry == nil {
		return false
	}
	normalized, err := normalizeSourceLagAlertSource(source, registry.maxSourceNameBytes)
	if err != nil {
		return false
	}
	registry.mu.Lock()
	_, exists := registry.alerts[normalized]
	delete(registry.alerts, normalized)
	registry.mu.Unlock()
	return exists
}
