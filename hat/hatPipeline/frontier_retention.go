package hatPipeline

import (
	"context"
	"errors"
	"sort"
	"sync"
)

var (
	// ErrFrontierRetentionRegistryNil indicates a nil retention registry or
	// frontier source.
	ErrFrontierRetentionRegistryNil = errors.New("hatPipeline: frontier retention registry is nil")
	// ErrFrontierRetentionOptionsInvalid indicates an invalid lease bound.
	ErrFrontierRetentionOptionsInvalid = errors.New("hatPipeline: frontier retention options are invalid")
	// ErrFrontierRetentionClosed indicates that the retention registry is closed.
	ErrFrontierRetentionClosed = errors.New("hatPipeline: frontier retention registry is closed")
	// ErrFrontierRetentionLeaseLimit indicates that the registry is full.
	ErrFrontierRetentionLeaseLimit = errors.New("hatPipeline: frontier retention lease limit reached")
	// ErrFrontierRetentionExpired indicates that a requested timestamp is
	// already below the frontier's completed lower bound.
	ErrFrontierRetentionExpired = errors.New("hatPipeline: frontier retention timestamp is expired")
	// ErrFrontierRetentionAhead indicates that a requested timestamp is above
	// the frontier's available upper bound.
	ErrFrontierRetentionAhead = errors.New("hatPipeline: frontier retention timestamp is ahead")
	// ErrFrontierRetentionLeaseInvalid indicates an empty lease identity.
	ErrFrontierRetentionLeaseInvalid = errors.New("hatPipeline: frontier retention lease is invalid")
	// ErrFrontierRetentionLeaseNotFound indicates an unknown or already
	// released lease.
	ErrFrontierRetentionLeaseNotFound = errors.New("hatPipeline: frontier retention lease is not found")
	// ErrFrontierRetentionPolicyViolation indicates that a requested history
	// age or observed storage size exceeds the configured policy.
	ErrFrontierRetentionPolicyViolation = errors.New("hatPipeline: frontier retention policy violation")
)

const (
	// DefaultFrontierRetentionMaxLeases bounds active as-of leases.
	DefaultFrontierRetentionMaxLeases = 1024
	maxFrontierRetentionLeases        = 1 << 20
)

// FrontierRetentionOptions bounds active retention leases.
type FrontierRetentionOptions struct {
	// MaxLeases is the maximum number of active leases. Zero uses
	// DefaultFrontierRetentionMaxLeases.
	MaxLeases int
}

// FrontierRetentionPolicy bounds one frontier's retained history. MaxAge is
// measured in logical timestamp units from the current upper frontier; zero
// disables the age bound. MaxBytes is caller-reported retained history size;
// zero disables the storage bound.
type FrontierRetentionPolicy struct {
	MaxAge   uint64
	MaxBytes uint64
}

// FrontierRetentionLease identifies one active as-of retention request.
type FrontierRetentionLease struct {
	ID         uint64
	FrontierID string
	AsOf       uint64
}

// FrontierRetentionSnapshot reports the active retention state for one
// frontier. SafeCompactionBefore is the largest boundary the caller may pass
// to a compactor that removes history strictly before that boundary.
type FrontierRetentionSnapshot struct {
	FrontierID           string
	LeaseCount           int
	MinimumRequired      uint64
	SafeCompactionBefore uint64
	CurrentLower         uint64
	CurrentUpper         uint64
	CompactionDebt       uint64
	BlockedByLease       bool
	BlockingLeaseCount   int
	Policy               FrontierRetentionPolicy
	StoredBytes          uint64
}

type frontierRetentionState struct {
	leases             map[uint64]FrontierRetentionLease
	minimum            uint64
	blockingLeaseCount int
}

type frontierRetentionPolicyState struct {
	policy      FrontierRetentionPolicy
	storedBytes uint64
}

// FrontierRetentionRegistry coordinates bounded historical-read leases with a
// FrontierRegistry. It does not compact data itself; callers must consult the
// safe boundary before deleting historical versions.
type FrontierRetentionRegistry struct {
	frontiers  *FrontierRegistry
	mu         sync.RWMutex
	maxLeases  int
	nextID     uint64
	leaseCount int
	closed     bool
	states     map[string]*frontierRetentionState
	policies   map[string]*frontierRetentionPolicyState
	notify     chan struct{}
}

// NewFrontierRetentionRegistry creates an as-of retention registry bound to
// frontiers.
func NewFrontierRetentionRegistry(frontiers *FrontierRegistry, options FrontierRetentionOptions) (*FrontierRetentionRegistry, error) {
	if frontiers == nil {
		return nil, ErrFrontierRetentionRegistryNil
	}
	maxLeases := options.MaxLeases
	if maxLeases == 0 {
		maxLeases = DefaultFrontierRetentionMaxLeases
	}
	if maxLeases < 1 || maxLeases > maxFrontierRetentionLeases {
		return nil, ErrFrontierRetentionOptionsInvalid
	}
	return &FrontierRetentionRegistry{
		frontiers: frontiers,
		maxLeases: maxLeases,
		states:    make(map[string]*frontierRetentionState),
	}, nil
}

// Acquire retains the requested as-of timestamp until Release is called. The
// timestamp must be within the frontier's current lower/upper bounds.
func (registry *FrontierRetentionRegistry) Acquire(frontierID string, asOf uint64) (FrontierRetentionLease, error) {
	if registry == nil {
		return FrontierRetentionLease{}, ErrFrontierRetentionRegistryNil
	}
	if frontierID == "" {
		return FrontierRetentionLease{}, ErrFrontierIDEmpty
	}
	if err := registry.checkTimestamp(frontierID, asOf); err != nil {
		return FrontierRetentionLease{}, err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return FrontierRetentionLease{}, ErrFrontierRetentionClosed
	}
	if registry.leaseCount >= registry.maxLeases {
		return FrontierRetentionLease{}, ErrFrontierRetentionLeaseLimit
	}
	if err := registry.checkTimestamp(frontierID, asOf); err != nil {
		return FrontierRetentionLease{}, err
	}
	registry.nextID++
	if registry.nextID == 0 {
		registry.nextID++
	}
	lease := FrontierRetentionLease{ID: registry.nextID, FrontierID: frontierID, AsOf: asOf}
	state := registry.states[frontierID]
	if state == nil {
		state = &frontierRetentionState{leases: make(map[uint64]FrontierRetentionLease)}
		registry.states[frontierID] = state
	} else if err := registry.checkPolicyLocked(frontierID, asOf); err != nil {
		return FrontierRetentionLease{}, err
	}
	state.leases[lease.ID] = lease
	if len(state.leases) == 1 || asOf < state.minimum {
		state.minimum = asOf
		state.blockingLeaseCount = 1
	} else if asOf == state.minimum {
		state.blockingLeaseCount++
	}
	registry.leaseCount++
	return lease, nil
}

// SetPolicy installs or replaces the bounded retention policy for one
// frontier. A zero policy clears the policy. It does not rewrite history or
// change existing leases; future acquisitions observe the new age bound.
func (registry *FrontierRetentionRegistry) SetPolicy(frontierID string, policy FrontierRetentionPolicy) error {
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
	var policyState *frontierRetentionPolicyState
	if registry.policies != nil {
		policyState = registry.policies[frontierID]
	}
	if policy.MaxAge == 0 && policy.MaxBytes == 0 {
		if policyState == nil {
			return nil
		}
		policyState.policy = FrontierRetentionPolicy{}
		if policyState.storedBytes == 0 {
			delete(registry.policies, frontierID)
			if len(registry.policies) == 0 {
				registry.policies = nil
			}
		}
		if state := registry.states[frontierID]; state != nil && len(state.leases) == 0 {
			delete(registry.states, frontierID)
		}
		registry.signalLocked()
		return nil
	}
	if policyState != nil && policy.MaxBytes > 0 && policyState.storedBytes > policy.MaxBytes {
		return ErrFrontierRetentionPolicyViolation
	}
	if policyState == nil {
		if registry.policies == nil {
			registry.policies = make(map[string]*frontierRetentionPolicyState)
		}
		policyState = &frontierRetentionPolicyState{}
		registry.policies[frontierID] = policyState
	}
	policyState.policy = policy
	if registry.states[frontierID] == nil {
		registry.states[frontierID] = &frontierRetentionState{leases: make(map[uint64]FrontierRetentionLease)}
	}
	registry.signalLocked()
	return nil
}

// ClearPolicy removes a frontier's policy while retaining any active lease or
// observed storage accounting.
func (registry *FrontierRetentionRegistry) ClearPolicy(frontierID string) error {
	return registry.SetPolicy(frontierID, FrontierRetentionPolicy{})
}

// ObserveStorage records caller-estimated retained history bytes for one
// frontier. The value is accepted atomically only when it fits MaxBytes.
func (registry *FrontierRetentionRegistry) ObserveStorage(frontierID string, bytes uint64) error {
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
	var policyState *frontierRetentionPolicyState
	if registry.policies != nil {
		policyState = registry.policies[frontierID]
	}
	if policyState != nil && policyState.policy.MaxBytes > 0 && bytes > policyState.policy.MaxBytes {
		return ErrFrontierRetentionPolicyViolation
	}
	if policyState == nil {
		if bytes == 0 {
			return nil
		}
		if registry.policies == nil {
			registry.policies = make(map[string]*frontierRetentionPolicyState)
		}
		policyState = &frontierRetentionPolicyState{}
		registry.policies[frontierID] = policyState
	}
	policyState.storedBytes = bytes
	if policyState.policy == (FrontierRetentionPolicy{}) && bytes == 0 {
		delete(registry.policies, frontierID)
		if len(registry.policies) == 0 {
			registry.policies = nil
		}
	}
	registry.signalLocked()
	return nil
}

// Release removes one active lease. Releasing the same lease twice returns
// ErrFrontierRetentionLeaseNotFound so ownership mistakes are visible.
func (registry *FrontierRetentionRegistry) Release(lease FrontierRetentionLease) error {
	if registry == nil {
		return ErrFrontierRetentionRegistryNil
	}
	if lease.ID == 0 || lease.FrontierID == "" {
		return ErrFrontierRetentionLeaseInvalid
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrFrontierRetentionClosed
	}
	state := registry.states[lease.FrontierID]
	if state == nil || state.leases[lease.ID] != lease {
		return ErrFrontierRetentionLeaseNotFound
	}
	delete(state.leases, lease.ID)
	registry.leaseCount--
	if len(state.leases) == 0 {
		keepState := false
		if registry.policies != nil {
			if policyState := registry.policies[lease.FrontierID]; policyState != nil {
				keepState = policyState.policy != (FrontierRetentionPolicy{})
			}
		}
		if !keepState {
			delete(registry.states, lease.FrontierID)
		}
		registry.signalLocked()
		return nil
	}
	if lease.AsOf == state.minimum {
		if state.blockingLeaseCount > 1 {
			state.blockingLeaseCount--
			registry.signalLocked()
			return nil
		}
		state.minimum = 0
		state.blockingLeaseCount = 0
		for _, active := range state.leases {
			if state.blockingLeaseCount == 0 || active.AsOf < state.minimum {
				state.minimum = active.AsOf
				state.blockingLeaseCount = 1
			} else if active.AsOf == state.minimum {
				state.blockingLeaseCount++
			}
		}
	}
	registry.signalLocked()
	return nil
}

// SafeCompactionBefore returns the largest boundary that preserves all active
// leases and does not pass the frontier's completed lower bound.
func (registry *FrontierRetentionRegistry) SafeCompactionBefore(frontierID string) (uint64, error) {
	if registry == nil {
		return 0, ErrFrontierRetentionRegistryNil
	}
	if frontierID == "" {
		return 0, ErrFrontierIDEmpty
	}
	registry.mu.RLock()
	if registry.closed {
		registry.mu.RUnlock()
		return 0, ErrFrontierRetentionClosed
	}
	state := registry.states[frontierID]
	minimum := uint64(0)
	hasState := state != nil && len(state.leases) > 0
	if hasState {
		minimum = state.minimum
	}
	registry.mu.RUnlock()
	snapshot, err := registry.frontierSnapshot(frontierID)
	if err != nil {
		return 0, err
	}
	safe := snapshot.Lower
	if hasState && minimum < safe {
		safe = minimum
	}
	return safe, nil
}

// CanCompactBefore reports whether history strictly before boundary may be
// removed without violating the current frontier or any active lease.
func (registry *FrontierRetentionRegistry) CanCompactBefore(frontierID string, boundary uint64) (bool, error) {
	safe, err := registry.SafeCompactionBefore(frontierID)
	if err != nil {
		return false, err
	}
	return boundary <= safe, nil
}

// WaitUntilSafe waits until history strictly before boundary may be removed
// for frontierID. It wakes on frontier progress and retention-lease release;
// it does not consume a Scheduler worker while waiting.
func (registry *FrontierRetentionRegistry) WaitUntilSafe(ctx context.Context, frontierID string, boundary uint64) error {
	if registry == nil {
		return ErrFrontierRetentionRegistryNil
	}
	if frontierID == "" {
		return ErrFrontierIDEmpty
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		safe, err := registry.SafeCompactionBefore(frontierID)
		if err != nil {
			return err
		}
		if boundary <= safe {
			return nil
		}
		frontier, err := registry.frontierSnapshot(frontierID)
		if err != nil {
			return err
		}
		if frontier.Lower < boundary {
			if err := registry.frontiers.WaitUntil(ctx, frontierID, boundary); err != nil {
				return err
			}
			continue
		}
		notify, wait, err := registry.retentionWaitChannel(frontierID, boundary)
		if err != nil {
			return err
		}
		if !wait {
			continue
		}
		select {
		case <-notify:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Snapshot reports active retention for one registered frontier.
func (registry *FrontierRetentionRegistry) Snapshot(frontierID string) (FrontierRetentionSnapshot, error) {
	if registry == nil {
		return FrontierRetentionSnapshot{}, ErrFrontierRetentionRegistryNil
	}
	if frontierID == "" {
		return FrontierRetentionSnapshot{}, ErrFrontierIDEmpty
	}
	registry.mu.RLock()
	if registry.closed {
		registry.mu.RUnlock()
		return FrontierRetentionSnapshot{}, ErrFrontierRetentionClosed
	}
	state := registry.states[frontierID]
	leaseCount := 0
	minimum := uint64(0)
	blockingLeaseCount := 0
	policy := FrontierRetentionPolicy{}
	storedBytes := uint64(0)
	hasState := state != nil && len(state.leases) > 0
	var policyState *frontierRetentionPolicyState
	if registry.policies != nil {
		policyState = registry.policies[frontierID]
	}
	if state != nil {
		leaseCount = len(state.leases)
		minimum = state.minimum
		blockingLeaseCount = state.blockingLeaseCount
	}
	if policyState != nil {
		policy = policyState.policy
		storedBytes = policyState.storedBytes
	}
	registry.mu.RUnlock()
	frontier, err := registry.frontierSnapshot(frontierID)
	if err != nil {
		return FrontierRetentionSnapshot{}, err
	}
	safe := frontier.Lower
	if hasState && minimum < safe {
		safe = minimum
	}
	if !hasState {
		minimum = frontier.Lower
	}
	debt := uint64(0)
	blockedByLease := false
	if safe < frontier.Lower {
		debt = frontier.Lower - safe
		blockedByLease = hasState && minimum < frontier.Lower
	}
	if !blockedByLease {
		blockingLeaseCount = 0
	}
	return FrontierRetentionSnapshot{
		FrontierID:           frontierID,
		LeaseCount:           leaseCount,
		MinimumRequired:      minimum,
		SafeCompactionBefore: safe,
		CurrentLower:         frontier.Lower,
		CurrentUpper:         frontier.Upper,
		CompactionDebt:       debt,
		BlockedByLease:       blockedByLease,
		BlockingLeaseCount:   blockingLeaseCount,
		Policy:               policy,
		StoredBytes:          storedBytes,
	}, nil
}

// ActiveLeases returns a detached, deterministic copy of the active leases for
// one registered frontier. The returned slice and lease values may be changed
// by the caller without affecting retention state.
func (registry *FrontierRetentionRegistry) ActiveLeases(frontierID string) ([]FrontierRetentionLease, error) {
	if registry == nil {
		return nil, ErrFrontierRetentionRegistryNil
	}
	if frontierID == "" {
		return nil, ErrFrontierIDEmpty
	}
	registry.mu.RLock()
	if registry.closed {
		registry.mu.RUnlock()
		return nil, ErrFrontierRetentionClosed
	}
	state := registry.states[frontierID]
	var leases []FrontierRetentionLease
	if state != nil {
		leases = make([]FrontierRetentionLease, 0, len(state.leases))
		for _, lease := range state.leases {
			leases = append(leases, lease)
		}
	}
	registry.mu.RUnlock()
	if _, err := registry.frontierSnapshot(frontierID); err != nil {
		return nil, err
	}
	if len(leases) > 1 {
		sort.Slice(leases, func(i, j int) bool { return leases[i].ID < leases[j].ID })
	}
	return leases, nil
}

// SnapshotAll returns retention state sorted by frontier ID.
func (registry *FrontierRetentionRegistry) SnapshotAll() []FrontierRetentionSnapshot {
	if registry == nil {
		return []FrontierRetentionSnapshot{}
	}
	frontiers := registry.frontiers.SnapshotAll()
	snapshots := make([]FrontierRetentionSnapshot, 0, len(frontiers))
	for _, frontier := range frontiers {
		if snapshot, err := registry.Snapshot(frontier.ID); err == nil {
			snapshots = append(snapshots, snapshot)
		}
	}
	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].FrontierID < snapshots[j].FrontierID })
	return snapshots
}

// Close rejects new leases and releases the registry's retained bookkeeping.
func (registry *FrontierRetentionRegistry) Close() error {
	if registry == nil {
		return nil
	}
	registry.mu.Lock()
	if registry.closed {
		registry.mu.Unlock()
		return nil
	}
	registry.closed = true
	registry.states = nil
	registry.policies = nil
	registry.leaseCount = 0
	registry.signalLocked()
	registry.mu.Unlock()
	return nil
}

func (registry *FrontierRetentionRegistry) retentionWaitChannel(frontierID string, boundary uint64) (<-chan struct{}, bool, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return nil, false, ErrFrontierRetentionClosed
	}
	state := registry.states[frontierID]
	if state == nil || len(state.leases) == 0 || state.minimum >= boundary {
		return nil, false, nil
	}
	if registry.notify == nil {
		registry.notify = make(chan struct{})
	}
	return registry.notify, true, nil
}

func (registry *FrontierRetentionRegistry) signalLocked() {
	if registry.notify != nil {
		close(registry.notify)
		registry.notify = nil
	}
}

func (registry *FrontierRetentionRegistry) checkPolicyLocked(frontierID string, asOf uint64) error {
	if registry.policies == nil {
		return nil
	}
	state := registry.policies[frontierID]
	if state == nil || state.policy == (FrontierRetentionPolicy{}) {
		return nil
	}
	if state.policy.MaxBytes > 0 && state.storedBytes > state.policy.MaxBytes {
		return ErrFrontierRetentionPolicyViolation
	}
	if state.policy.MaxAge == 0 {
		return nil
	}
	snapshot, err := registry.frontierSnapshot(frontierID)
	if err != nil {
		return err
	}
	if snapshot.Upper >= asOf && snapshot.Upper-asOf > state.policy.MaxAge {
		return ErrFrontierRetentionPolicyViolation
	}
	return nil
}

func (registry *FrontierRetentionRegistry) checkTimestamp(frontierID string, asOf uint64) error {
	snapshot, err := registry.frontierSnapshot(frontierID)
	if err != nil {
		return err
	}
	if asOf < snapshot.Lower {
		return ErrFrontierRetentionExpired
	}
	if asOf > snapshot.Upper {
		return ErrFrontierRetentionAhead
	}
	return nil
}

func (registry *FrontierRetentionRegistry) frontierSnapshot(frontierID string) (FrontierSnapshot, error) {
	object, err := registry.frontiers.object(frontierID)
	if err != nil {
		return FrontierSnapshot{}, err
	}
	return object.snapshot(), nil
}
