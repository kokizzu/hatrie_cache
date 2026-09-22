package hatReplication

import (
	"errors"
	"math/bits"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultReplicationProgressMetricsMaxTargets bounds the default per-node
	// metric map while leaving room for ordinary replica sets.
	DefaultReplicationProgressMetricsMaxTargets = 64
	maxReplicationProgressMetricsTargets        = 4096
)

var (
	ErrReplicationProgressMetricsDisabled       = errors.New("replication progress metrics are disabled")
	ErrReplicationProgressMetricsInvalidOptions = errors.New("replication progress metrics options are invalid")
	ErrReplicationProgressMetricsInvalidTarget  = errors.New("replication progress metrics target is invalid")
	ErrReplicationProgressMetricsInvalidSample  = errors.New("replication progress metrics sample is invalid")
	ErrReplicationProgressMetricsTargetLimit    = errors.New("replication progress metrics target limit reached")
	ErrReplicationProgressMetricsRegressed      = errors.New("replication progress metrics progress regressed")
	ErrReplicationProgressMetricsTimestamp      = errors.New("replication progress metrics timestamp regressed")
)

// ReplicationProgressMetricsOptions configures the opt-in progress collector.
// MaxTargets defaults to DefaultReplicationProgressMetricsMaxTargets and is
// capped to keep metric memory bounded.
type ReplicationProgressMetricsOptions struct {
	Enabled    bool
	MaxTargets int
}

// ReplicationProgressObservation reports one target's journal progress. LSN
// values are compared as monotone journal positions; AppliedBytes is optional
// and may remain zero when the applier does not expose byte accounting.
type ReplicationProgressObservation struct {
	TargetID     string
	SourceLSN    uint64
	AppliedLSN   uint64
	AppliedBytes uint64
	ObservedAt   time.Time
}

// ReplicationTargetProgressMetrics is the detached metric view for one target.
type ReplicationTargetProgressMetrics struct {
	TargetID            string    `json:"target_id"`
	AppliedLSN          uint64    `json:"applied_lsn"`
	LagLSN              uint64    `json:"lag_lsn"`
	AppliedBytes        uint64    `json:"applied_bytes"`
	ApplyLSNPerSecond   uint64    `json:"apply_lsn_per_second"`
	ApplyBytesPerSecond uint64    `json:"apply_bytes_per_second"`
	LastObservedAt      time.Time `json:"last_observed_at"`
}

// ReplicationProgressMetricsSnapshot is a detached, consistently ordered
// view suitable for monitoring responses and periodic export.
type ReplicationProgressMetricsSnapshot struct {
	Enabled   bool                               `json:"enabled"`
	SourceLSN uint64                             `json:"source_lsn"`
	Targets   []ReplicationTargetProgressMetrics `json:"targets,omitempty"`
}

type replicationTargetProgressMetrics struct {
	appliedLSN          uint64
	appliedBytes        uint64
	applyLSNPerSecond   uint64
	applyBytesPerSecond uint64
	lastObservedAt      time.Time
}

// ReplicationProgressMetrics derives bounded lag and apply-throughput metrics
// from source and replica LSN observations. A disabled collector has no
// internal map and returns ErrReplicationProgressMetricsDisabled from Observe.
type ReplicationProgressMetrics struct {
	mu         sync.RWMutex
	enabled    bool
	maxTargets int
	sourceLSN  uint64
	targets    map[string]replicationTargetProgressMetrics
}

// NewReplicationProgressMetrics creates an opt-in progress collector.
func NewReplicationProgressMetrics(options ReplicationProgressMetricsOptions) (*ReplicationProgressMetrics, error) {
	maxTargets := options.MaxTargets
	if maxTargets == 0 {
		maxTargets = DefaultReplicationProgressMetricsMaxTargets
	}
	if maxTargets < 0 || maxTargets > maxReplicationProgressMetricsTargets {
		return nil, ErrReplicationProgressMetricsInvalidOptions
	}
	return &ReplicationProgressMetrics{
		enabled:    options.Enabled,
		maxTargets: maxTargets,
	}, nil
}

// Observe records one target sample and derives rates from its previous
// sample. A lower source LSN is accepted for a stale target observation, but
// the collector's source LSN remains a global high-water mark.
func (metrics *ReplicationProgressMetrics) Observe(observation ReplicationProgressObservation) error {
	if metrics == nil {
		return ErrReplicationProgressMetricsInvalidOptions
	}
	metrics.mu.Lock()
	defer metrics.mu.Unlock()

	if !metrics.enabled {
		return ErrReplicationProgressMetricsDisabled
	}
	targetID := strings.TrimSpace(observation.TargetID)
	if targetID == "" {
		return ErrReplicationProgressMetricsInvalidTarget
	}
	if observation.ObservedAt.IsZero() || observation.AppliedLSN > observation.SourceLSN {
		return ErrReplicationProgressMetricsInvalidSample
	}

	previous, exists := metrics.targets[targetID]
	if exists {
		if observation.ObservedAt.Before(previous.lastObservedAt) {
			return ErrReplicationProgressMetricsTimestamp
		}
		if observation.AppliedLSN < previous.appliedLSN || observation.AppliedBytes < previous.appliedBytes {
			return ErrReplicationProgressMetricsRegressed
		}
		if observation.ObservedAt.Equal(previous.lastObservedAt) {
			if observation.AppliedLSN != previous.appliedLSN || observation.AppliedBytes != previous.appliedBytes {
				return ErrReplicationProgressMetricsTimestamp
			}
			if observation.SourceLSN > metrics.sourceLSN {
				metrics.sourceLSN = observation.SourceLSN
			}
			return nil
		}
		elapsed := observation.ObservedAt.Sub(previous.lastObservedAt)
		previous.applyLSNPerSecond = replicationProgressRatePerSecond(observation.AppliedLSN-previous.appliedLSN, elapsed)
		previous.applyBytesPerSecond = replicationProgressRatePerSecond(observation.AppliedBytes-previous.appliedBytes, elapsed)
	}

	if !exists {
		if len(metrics.targets) >= metrics.maxTargets {
			return ErrReplicationProgressMetricsTargetLimit
		}
		if metrics.targets == nil {
			metrics.targets = make(map[string]replicationTargetProgressMetrics, metrics.maxTargets)
		}
	}
	if observation.SourceLSN > metrics.sourceLSN {
		metrics.sourceLSN = observation.SourceLSN
	}
	metrics.targets[targetID] = replicationTargetProgressMetrics{
		appliedLSN:          observation.AppliedLSN,
		appliedBytes:        observation.AppliedBytes,
		applyLSNPerSecond:   previous.applyLSNPerSecond,
		applyBytesPerSecond: previous.applyBytesPerSecond,
		lastObservedAt:      observation.ObservedAt,
	}
	return nil
}

// Snapshot returns a detached target-sorted view. Lag is calculated against
// the collector's global source high-water mark at snapshot time.
func (metrics *ReplicationProgressMetrics) Snapshot() ReplicationProgressMetricsSnapshot {
	if metrics == nil {
		return ReplicationProgressMetricsSnapshot{}
	}
	metrics.mu.RLock()
	defer metrics.mu.RUnlock()

	snapshot := ReplicationProgressMetricsSnapshot{
		Enabled:   metrics.enabled,
		SourceLSN: metrics.sourceLSN,
	}
	if !metrics.enabled || len(metrics.targets) == 0 {
		return snapshot
	}
	ids := make([]string, 0, len(metrics.targets))
	for targetID := range metrics.targets {
		ids = append(ids, targetID)
	}
	sort.Strings(ids)
	snapshot.Targets = make([]ReplicationTargetProgressMetrics, 0, len(ids))
	for _, targetID := range ids {
		progress := metrics.targets[targetID]
		snapshot.Targets = append(snapshot.Targets, ReplicationTargetProgressMetrics{
			TargetID:            targetID,
			AppliedLSN:          progress.appliedLSN,
			LagLSN:              replicationProgressLag(metrics.sourceLSN, progress.appliedLSN),
			AppliedBytes:        progress.appliedBytes,
			ApplyLSNPerSecond:   progress.applyLSNPerSecond,
			ApplyBytesPerSecond: progress.applyBytesPerSecond,
			LastObservedAt:      progress.lastObservedAt,
		})
	}
	return snapshot
}

func replicationProgressLag(sourceLSN, appliedLSN uint64) uint64 {
	if appliedLSN >= sourceLSN {
		return 0
	}
	return sourceLSN - appliedLSN
}

func replicationProgressRatePerSecond(delta uint64, elapsed time.Duration) uint64 {
	if delta == 0 || elapsed <= 0 {
		return 0
	}
	nanos := uint64(elapsed)
	high, low := bits.Mul64(delta, uint64(time.Second))
	if high >= nanos {
		return ^uint64(0)
	}
	quotient, _ := bits.Div64(high, low, nanos)
	return quotient
}
