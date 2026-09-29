package hatReplication

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	ErrReplicaLSNMetricsNotInitialized = errors.New("hatriecache: replica LSN metrics is not initialized")
	ErrReplicaLSNMetricsSpaceRequired  = errors.New("hatriecache: replica LSN metrics space is required")
	ErrReplicaLSNMetricsInvalid        = errors.New("hatriecache: replica LSN metrics options are invalid")
	ErrReplicaLSNMetricsLimit          = errors.New("hatriecache: replica LSN metrics space limit reached")
	ErrReplicaLSNMetricsRegressed      = errors.New("hatriecache: replica LSN metrics observation regressed")
)

const (
	DefaultReplicaLSNMetricsMaxSpaces = 256
	MaxReplicaLSNMetricsMaxSpaces     = 65536
	MaxReplicaLSNMetricsSpaceBytes    = 256
)

// ReplicaLSNMetricsOptions bounds the number of independently tracked spaces.
// Zero selects DefaultReplicaLSNMetricsMaxSpaces. The tracker is opt-in and
// does not install itself into a replication transport.
type ReplicaLSNMetricsOptions struct {
	MaxSpaces int
}

// ReplicaLSNSnapshot is the latest bounded replication progress for one space.
// LSN values are caller-defined monotone positions, normally WAL or changefeed
// sequence numbers. Lag is saturated at zero when the sampled source position
// is behind the applied position.
type ReplicaLSNSnapshot struct {
	Space                     string    `json:"space"`
	SourceLSN                 uint64    `json:"source_lsn"`
	AppliedLSN                uint64    `json:"applied_lsn"`
	LagLSN                    uint64    `json:"lag_lsn"`
	SourceThroughputPerSecond float64   `json:"source_throughput_per_second"`
	ApplyThroughputPerSecond  float64   `json:"apply_throughput_per_second"`
	ObservedAt                time.Time `json:"observed_at"`
}

type replicaLSNState struct {
	ReplicaLSNSnapshot
}

// ReplicaLSNMetrics tracks bounded, per-space source and applied LSNs. It is
// safe for concurrent observers and snapshots. A failed observation leaves the
// previous state unchanged.
type ReplicaLSNMetrics struct {
	mu        sync.RWMutex
	maxSpaces int
	spaces    map[string]replicaLSNState
}

// NewReplicaLSNMetrics creates an empty bounded LSN tracker.
func NewReplicaLSNMetrics(options ReplicaLSNMetricsOptions) (*ReplicaLSNMetrics, error) {
	maxSpaces := options.MaxSpaces
	if maxSpaces == 0 {
		maxSpaces = DefaultReplicaLSNMetricsMaxSpaces
	}
	if maxSpaces < 1 || maxSpaces > MaxReplicaLSNMetricsMaxSpaces {
		return nil, ErrReplicaLSNMetricsInvalid
	}
	return &ReplicaLSNMetrics{
		maxSpaces: maxSpaces,
		spaces:    make(map[string]replicaLSNState, maxSpaces),
	}, nil
}

// Observe records one source/applier watermark pair. The first observation
// reports zero throughput. Later rates use LSN deltas divided by elapsed wall
// time; equal timestamps keep the rate at zero. Source and applied LSNs and
// timestamps must not regress for an existing space.
func (metrics *ReplicaLSNMetrics) Observe(space string, sourceLSN, appliedLSN uint64, observedAt time.Time) (ReplicaLSNSnapshot, error) {
	if metrics == nil {
		return ReplicaLSNSnapshot{}, ErrReplicaLSNMetricsNotInitialized
	}
	space = strings.TrimSpace(space)
	if space == "" {
		return ReplicaLSNSnapshot{}, ErrReplicaLSNMetricsSpaceRequired
	}
	if len(space) > MaxReplicaLSNMetricsSpaceBytes || strings.IndexByte(space, 0) >= 0 {
		return ReplicaLSNSnapshot{}, ErrReplicaLSNMetricsInvalid
	}
	if observedAt.IsZero() {
		observedAt = time.Now()
	}

	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	if metrics.maxSpaces < 1 || metrics.spaces == nil {
		return ReplicaLSNSnapshot{}, ErrReplicaLSNMetricsNotInitialized
	}
	state, exists := metrics.spaces[space]
	if exists {
		if sourceLSN < state.SourceLSN || appliedLSN < state.AppliedLSN || observedAt.Before(state.ObservedAt) {
			return ReplicaLSNSnapshot{}, ErrReplicaLSNMetricsRegressed
		}
	} else if len(metrics.spaces) >= metrics.maxSpaces {
		return ReplicaLSNSnapshot{}, ErrReplicaLSNMetricsLimit
	}

	sourceRate := float64(0)
	applyRate := float64(0)
	if exists {
		elapsed := observedAt.Sub(state.ObservedAt).Seconds()
		if elapsed > 0 {
			sourceRate = float64(sourceLSN-state.SourceLSN) / elapsed
			applyRate = float64(appliedLSN-state.AppliedLSN) / elapsed
		}
	}
	lag := uint64(0)
	if sourceLSN > appliedLSN {
		lag = sourceLSN - appliedLSN
	}
	storedSpace := space
	if !exists {
		storedSpace = strings.Clone(space)
	}
	state = replicaLSNState{ReplicaLSNSnapshot{
		Space:                     storedSpace,
		SourceLSN:                 sourceLSN,
		AppliedLSN:                appliedLSN,
		LagLSN:                    lag,
		SourceThroughputPerSecond: sourceRate,
		ApplyThroughputPerSecond:  applyRate,
		ObservedAt:                observedAt,
	}}
	metrics.spaces[space] = state
	return state.ReplicaLSNSnapshot, nil
}

// Snapshot returns independent, lexicographically ordered status values.
// Returning a sorted slice keeps exports and tests deterministic without
// imposing ordering or allocation work on Observe.
func (metrics *ReplicaLSNMetrics) Snapshot() []ReplicaLSNSnapshot {
	if metrics == nil {
		return nil
	}
	metrics.mu.RLock()
	defer metrics.mu.RUnlock()
	if len(metrics.spaces) == 0 {
		return nil
	}
	result := make([]ReplicaLSNSnapshot, 0, len(metrics.spaces))
	for _, state := range metrics.spaces {
		result = append(result, state.ReplicaLSNSnapshot)
	}
	sort.Slice(result, func(left, right int) bool {
		return result[left].Space < result[right].Space
	})
	return result
}
