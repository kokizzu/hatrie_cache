package hatPipeline

import (
	"errors"
	"sort"
)

var (
	// ErrFrontierRetentionPolicyInvalid indicates that a policy has no active
	// bound.
	ErrFrontierRetentionPolicyInvalid = errors.New("hatPipeline: frontier retention policy is invalid")
	// ErrFrontierRetentionPolicyLimit indicates that the policy registry is
	// full.
	ErrFrontierRetentionPolicyLimit = errors.New("hatPipeline: frontier retention policy limit reached")
	// ErrFrontierRetentionPolicyNotFound indicates that no policy is configured
	// for the requested frontier.
	ErrFrontierRetentionPolicyNotFound = errors.New("hatPipeline: frontier retention policy is not found")
)

const (
	// DefaultFrontierRetentionMaxPolicies bounds configured per-frontier
	// policies when the caller does not provide a limit.
	DefaultFrontierRetentionMaxPolicies = 1024
	maxFrontierRetentionPolicies        = 1 << 20
)

// FrontierRetentionPolicy bounds caller-reported retained history for one
// frontier. A zero field disables that individual bound; at least one field
// must be non-zero when configuring a policy.
type FrontierRetentionPolicy struct {
	MaxHistory uint64
	MaxBytes   uint64
}

// FrontierRetentionUsage is a caller-supplied estimate of retained history
// for one frontier. RetainedHistory is expressed in the frontier's logical
// timestamp units, while RetainedBytes is an estimated storage size.
type FrontierRetentionUsage struct {
	RetainedHistory uint64
	RetainedBytes   uint64
}

// FrontierRetentionPolicySnapshot reports one frontier's configured policy
// and caller-reported usage. It is separate from FrontierRetentionSnapshot so
// the existing compaction-monitoring path keeps its original size and cost.
type FrontierRetentionPolicySnapshot struct {
	FrontierID            string
	PolicyConfigured      bool
	MaxRetainedHistory    uint64
	MaxRetainedBytes      uint64
	RetainedHistory       uint64
	RetainedBytes         uint64
	HistoryBudgetExceeded bool
	BytesBudgetExceeded   bool
	BudgetExceeded        bool
}

type frontierRetentionPolicyState struct {
	policy FrontierRetentionPolicy
	usage  FrontierRetentionUsage
}

// SetPolicy configures or replaces the bounded retention policy for one
// registered frontier. It does not compact data or start a worker.
func (registry *FrontierRetentionRegistry) SetPolicy(frontierID string, policy FrontierRetentionPolicy) error {
	if registry == nil {
		return ErrFrontierRetentionRegistryNil
	}
	if frontierID == "" {
		return ErrFrontierIDEmpty
	}
	if policy.MaxHistory == 0 && policy.MaxBytes == 0 {
		return ErrFrontierRetentionPolicyInvalid
	}
	if _, err := registry.frontierSnapshot(frontierID); err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrFrontierRetentionClosed
	}
	if registry.policies == nil {
		registry.policies = make(map[string]frontierRetentionPolicyState)
	}
	state, exists := registry.policies[frontierID]
	if !exists && len(registry.policies) >= registry.maxPolicies {
		return ErrFrontierRetentionPolicyLimit
	}
	state.policy = policy
	registry.policies[frontierID] = state
	return nil
}

// SetUsage replaces the caller-reported usage for one configured frontier.
// It only updates existing bounded state and does not allocate per update.
func (registry *FrontierRetentionRegistry) SetUsage(frontierID string, usage FrontierRetentionUsage) error {
	if registry == nil {
		return ErrFrontierRetentionRegistryNil
	}
	if frontierID == "" {
		return ErrFrontierIDEmpty
	}
	if _, err := registry.frontierSnapshot(frontierID); err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrFrontierRetentionClosed
	}
	state, ok := registry.policies[frontierID]
	if !ok {
		return ErrFrontierRetentionPolicyNotFound
	}
	state.usage = usage
	registry.policies[frontierID] = state
	return nil
}

// PolicySnapshot returns one frontier's policy state. An unconfigured
// frontier returns a zero snapshot after its frontier ID is validated.
func (registry *FrontierRetentionRegistry) PolicySnapshot(frontierID string) (FrontierRetentionPolicySnapshot, error) {
	if registry == nil {
		return FrontierRetentionPolicySnapshot{}, ErrFrontierRetentionRegistryNil
	}
	if frontierID == "" {
		return FrontierRetentionPolicySnapshot{}, ErrFrontierIDEmpty
	}
	registry.mu.RLock()
	if registry.closed {
		registry.mu.RUnlock()
		return FrontierRetentionPolicySnapshot{}, ErrFrontierRetentionClosed
	}
	state, configured := registry.policies[frontierID]
	registry.mu.RUnlock()
	if _, err := registry.frontierSnapshot(frontierID); err != nil {
		return FrontierRetentionPolicySnapshot{}, err
	}
	snapshot := FrontierRetentionPolicySnapshot{FrontierID: frontierID, PolicyConfigured: configured}
	if !configured {
		return snapshot, nil
	}
	snapshot.MaxRetainedHistory = state.policy.MaxHistory
	snapshot.MaxRetainedBytes = state.policy.MaxBytes
	snapshot.RetainedHistory = state.usage.RetainedHistory
	snapshot.RetainedBytes = state.usage.RetainedBytes
	snapshot.HistoryBudgetExceeded = state.policy.MaxHistory > 0 && state.usage.RetainedHistory > state.policy.MaxHistory
	snapshot.BytesBudgetExceeded = state.policy.MaxBytes > 0 && state.usage.RetainedBytes > state.policy.MaxBytes
	snapshot.BudgetExceeded = snapshot.HistoryBudgetExceeded || snapshot.BytesBudgetExceeded
	return snapshot, nil
}

// PolicySnapshots returns configured policy state sorted by frontier ID.
func (registry *FrontierRetentionRegistry) PolicySnapshots() []FrontierRetentionPolicySnapshot {
	if registry == nil {
		return []FrontierRetentionPolicySnapshot{}
	}
	registry.mu.RLock()
	if registry.closed {
		registry.mu.RUnlock()
		return []FrontierRetentionPolicySnapshot{}
	}
	frontierIDs := make([]string, 0, len(registry.policies))
	for frontierID := range registry.policies {
		frontierIDs = append(frontierIDs, frontierID)
	}
	registry.mu.RUnlock()
	sort.Strings(frontierIDs)
	snapshots := make([]FrontierRetentionPolicySnapshot, 0, len(frontierIDs))
	for _, frontierID := range frontierIDs {
		if snapshot, err := registry.PolicySnapshot(frontierID); err == nil {
			snapshots = append(snapshots, snapshot)
		}
	}
	return snapshots
}

// ClearPolicy removes one configured policy and releases its bounded slot.
func (registry *FrontierRetentionRegistry) ClearPolicy(frontierID string) error {
	if registry == nil {
		return ErrFrontierRetentionRegistryNil
	}
	if frontierID == "" {
		return ErrFrontierIDEmpty
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrFrontierRetentionClosed
	}
	if _, ok := registry.policies[frontierID]; !ok {
		return ErrFrontierRetentionPolicyNotFound
	}
	delete(registry.policies, frontierID)
	if len(registry.policies) == 0 {
		registry.policies = nil
	}
	return nil
}
