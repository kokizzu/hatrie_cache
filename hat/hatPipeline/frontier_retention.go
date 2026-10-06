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
	// ErrFrontierRetentionCompactionInProgress indicates that an as-of lease
	// older than an active compaction boundary would be unsafe to acquire.
	ErrFrontierRetentionCompactionInProgress = errors.New("hatPipeline: frontier retention compaction is in progress")
	// ErrFrontierRetentionCompactionPermitNotFound indicates an invalid permit
	// release.
	ErrFrontierRetentionCompactionPermitNotFound = errors.New("hatPipeline: frontier retention compaction permit is not found")
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

// FrontierRetentionLease identifies one active as-of retention request.
type FrontierRetentionLease struct {
	ID         uint64
	FrontierID string
	AsOf       uint64
}

// FrontierCompactionPermit reserves one safe compaction boundary. Release is
// idempotent, so a storage callback can defer it without coordinating with a
// caller-owned cleanup path.
type FrontierCompactionPermit struct {
	registry   *FrontierRetentionRegistry
	frontierID string
	boundary   uint64
	noop       bool
	once       sync.Once
	releaseErr error
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
	CompactionBoundary   uint64
	CompactionActive     bool
}

type frontierRetentionState struct {
	leases             map[uint64]FrontierRetentionLease
	minimum            uint64
	blockingLeaseCount int
	compactionBoundary uint64
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
	if state := registry.states[frontierID]; state != nil && state.compactionBoundary != 0 && asOf < state.compactionBoundary {
		return FrontierRetentionLease{}, ErrFrontierRetentionCompactionInProgress
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

// BeginCompaction waits until history strictly before boundary is safe to
// remove, then reserves that boundary so newer as-of leases cannot re-open the
// unsafe range before the storage callback starts. The permit must be released
// after the callback completes.
func (registry *FrontierRetentionRegistry) BeginCompaction(ctx context.Context, frontierID string, boundary uint64) (*FrontierCompactionPermit, error) {
	if registry == nil {
		return nil, ErrFrontierRetentionRegistryNil
	}
	if frontierID == "" {
		return nil, ErrFrontierIDEmpty
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if boundary == 0 {
		return &FrontierCompactionPermit{registry: registry, frontierID: frontierID, noop: true}, nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		frontier, err := registry.frontierSnapshot(frontierID)
		if err != nil {
			return nil, err
		}
		if frontier.Lower < boundary {
			if err := registry.frontiers.WaitUntil(ctx, frontierID, boundary); err != nil {
				return nil, err
			}
			continue
		}

		registry.mu.Lock()
		if registry.closed {
			registry.mu.Unlock()
			return nil, ErrFrontierRetentionClosed
		}
		state := registry.states[frontierID]
		blocked := state != nil && (state.compactionBoundary != 0 || (len(state.leases) > 0 && state.minimum < boundary))
		if !blocked {
			if state == nil {
				state = &frontierRetentionState{leases: make(map[uint64]FrontierRetentionLease)}
				registry.states[frontierID] = state
			}
			state.compactionBoundary = boundary
			registry.mu.Unlock()
			return &FrontierCompactionPermit{registry: registry, frontierID: frontierID, boundary: boundary}, nil
		}
		if registry.notify == nil {
			registry.notify = make(chan struct{})
		}
		notify := registry.notify
		registry.mu.Unlock()
		select {
		case <-notify:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Release relinquishes the compaction reservation. It is safe to call more
// than once; all calls after the first return the first release result.
func (permit *FrontierCompactionPermit) Release() error {
	if permit == nil {
		return ErrFrontierRetentionCompactionPermitNotFound
	}
	permit.once.Do(func() {
		if permit.noop {
			return
		}
		if permit.registry == nil || permit.frontierID == "" {
			permit.releaseErr = ErrFrontierRetentionCompactionPermitNotFound
			return
		}
		permit.releaseErr = permit.registry.releaseCompaction(permit.frontierID, permit.boundary)
	})
	return permit.releaseErr
}

func (registry *FrontierRetentionRegistry) releaseCompaction(frontierID string, boundary uint64) error {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrFrontierRetentionClosed
	}
	state := registry.states[frontierID]
	if state == nil || state.compactionBoundary != boundary {
		return ErrFrontierRetentionCompactionPermitNotFound
	}
	state.compactionBoundary = 0
	if len(state.leases) == 0 {
		delete(registry.states, frontierID)
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
		delete(registry.states, lease.FrontierID)
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
	hasState := state != nil
	compactionActive := false
	if hasState {
		minimum = state.minimum
		compactionActive = state.compactionBoundary != 0
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
	if compactionActive {
		safe = 0
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
	hasState := state != nil
	compactionBoundary := uint64(0)
	compactionActive := false
	if state != nil {
		leaseCount = len(state.leases)
		minimum = state.minimum
		blockingLeaseCount = state.blockingLeaseCount
		compactionBoundary = state.compactionBoundary
		compactionActive = compactionBoundary != 0
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
	if compactionActive {
		safe = 0
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
		CompactionBoundary:   compactionBoundary,
		CompactionActive:     compactionActive,
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
	if state == nil || (state.compactionBoundary == 0 && state.minimum >= boundary) {
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
