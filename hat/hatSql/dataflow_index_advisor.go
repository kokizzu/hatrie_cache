package hatSql

import (
	"sort"
	"sync"
)

const (
	// DefaultSQLDataflowIndexAdvisorCapacity bounds retained candidate
	// arrangements when the advisor is constructed with zero capacity.
	DefaultSQLDataflowIndexAdvisorCapacity = 128
	// DefaultSQLDataflowIndexAdvisorMinSamples avoids recommendations from a
	// single potentially noisy dataflow observation.
	DefaultSQLDataflowIndexAdvisorMinSamples = 2
)

// SQLDataflowIndexAdvisorOptions configures bounded automatic recommendation
// generation from application-supplied dataflow observations.
type SQLDataflowIndexAdvisorOptions struct {
	Capacity   int
	MinSamples uint64
	CostModel  SQLArrangementCostModelOptions
}

// SQLDataflowIndexObservation describes one observed use of a candidate
// arrangement. ExpectedReads and ExpectedWrites are added to the retained
// candidate, while cost and memory fields are kept conservatively at their
// largest observed values.
type SQLDataflowIndexObservation struct {
	Candidate SQLArrangementCostCandidate
}

// SQLDataflowIndexRecommendation combines an aggregated candidate with its
// cost score and the number of observations that supported it.
type SQLDataflowIndexRecommendation struct {
	SQLArrangementCostScore
	Samples uint64
}

// SQLDataflowIndexAdvisor retains a bounded set of dataflow candidates and
// emits only candidates that meet the configured sample and cost thresholds.
// It never creates, drops, or changes an index.
type SQLDataflowIndexAdvisor struct {
	capacity   int
	minSamples uint64
	model      *SQLArrangementCostModel
	mu         sync.RWMutex
	candidates map[sqlDataflowIndexKey]sqlDataflowIndexStats
}

type sqlDataflowIndexKey struct {
	key   string
	field string
	kind  string
}

type sqlDataflowIndexStats struct {
	candidate SQLArrangementCostCandidate
	samples   uint64
}

// NewSQLDataflowIndexAdvisor creates an opt-in, bounded dataflow advisor.
// Zero Capacity and MinSamples use conservative defaults.
func NewSQLDataflowIndexAdvisor(options SQLDataflowIndexAdvisorOptions) *SQLDataflowIndexAdvisor {
	if options.Capacity <= 0 {
		options.Capacity = DefaultSQLDataflowIndexAdvisorCapacity
	}
	if options.MinSamples == 0 {
		options.MinSamples = DefaultSQLDataflowIndexAdvisorMinSamples
	}
	return &SQLDataflowIndexAdvisor{
		capacity:   options.Capacity,
		minSamples: options.MinSamples,
		model:      NewSQLArrangementCostModel(options.CostModel),
		candidates: make(map[sqlDataflowIndexKey]sqlDataflowIndexStats, options.Capacity),
	}
}

// Reset clears retained observations while preserving the advisor's bounded
// map capacity and configuration. It is useful for explicit workload windows.
func (advisor *SQLDataflowIndexAdvisor) Reset() {
	if advisor == nil {
		return
	}
	advisor.mu.Lock()
	clear(advisor.candidates)
	advisor.mu.Unlock()
}

// ObserveDataflow records one candidate observation. It returns false for an
// invalid identity or a new candidate after the retention bound is full.
func (advisor *SQLDataflowIndexAdvisor) ObserveDataflow(observation SQLDataflowIndexObservation) bool {
	if advisor == nil {
		return false
	}
	advisor.mu.Lock()
	accepted := advisor.observeDataflowLocked(&observation)
	advisor.mu.Unlock()
	return accepted
}

// ObserveDataflows records a batch of candidate observations under one lock
// and returns the number accepted. Input order and the same bounded admission
// rules as ObserveDataflow are preserved.
func (advisor *SQLDataflowIndexAdvisor) ObserveDataflows(observations []SQLDataflowIndexObservation) int {
	if advisor == nil || len(observations) == 0 {
		return 0
	}
	advisor.mu.Lock()
	accepted := 0
	for index := range observations {
		if advisor.observeDataflowLocked(&observations[index]) {
			accepted++
		}
	}
	advisor.mu.Unlock()
	return accepted
}

func (advisor *SQLDataflowIndexAdvisor) observeDataflowLocked(observation *SQLDataflowIndexObservation) bool {
	candidate := &observation.Candidate
	if candidate.Key == "" || candidate.Field == "" {
		return false
	}
	key := sqlDataflowIndexKey{key: candidate.Key, field: candidate.Field, kind: candidate.Kind}
	stats, exists := advisor.candidates[key]
	if !exists {
		if len(advisor.candidates) >= advisor.capacity {
			return false
		}
		stats.candidate = SQLArrangementCostCandidate{
			Key:   candidate.Key,
			Field: candidate.Field,
			Kind:  candidate.Kind,
		}
	}
	stats.samples = sqlDataflowIndexSaturatingAdd(stats.samples, 1)
	if candidate.BuildCostNanos > stats.candidate.BuildCostNanos {
		stats.candidate.BuildCostNanos = candidate.BuildCostNanos
	}
	if candidate.ProbeCostNanos > stats.candidate.ProbeCostNanos {
		stats.candidate.ProbeCostNanos = candidate.ProbeCostNanos
	}
	if candidate.ScanCostNanos > stats.candidate.ScanCostNanos {
		stats.candidate.ScanCostNanos = candidate.ScanCostNanos
	}
	if candidate.MaintenanceNanos > stats.candidate.MaintenanceNanos {
		stats.candidate.MaintenanceNanos = candidate.MaintenanceNanos
	}
	if candidate.MemoryBytes > stats.candidate.MemoryBytes {
		stats.candidate.MemoryBytes = candidate.MemoryBytes
	}
	stats.candidate.ExpectedReads = sqlDataflowIndexSaturatingAdd(stats.candidate.ExpectedReads, candidate.ExpectedReads)
	stats.candidate.ExpectedWrites = sqlDataflowIndexSaturatingAdd(stats.candidate.ExpectedWrites, candidate.ExpectedWrites)
	advisor.candidates[key] = stats
	return true
}

// Recommendations returns reusable candidates ordered by descending net
// benefit, ascending payback, and stable candidate identity. A nonpositive
// limit returns every reusable candidate.
func (advisor *SQLDataflowIndexAdvisor) Recommendations(limit int) []SQLDataflowIndexRecommendation {
	if advisor == nil {
		return nil
	}
	advisor.mu.RLock()
	recommendations := make([]SQLDataflowIndexRecommendation, 0, len(advisor.candidates))
	for _, stats := range advisor.candidates {
		if stats.samples < advisor.minSamples {
			continue
		}
		score := advisor.model.Evaluate(stats.candidate)
		if !score.Reusable {
			continue
		}
		recommendations = append(recommendations, SQLDataflowIndexRecommendation{
			SQLArrangementCostScore: score,
			Samples:                 stats.samples,
		})
	}
	advisor.mu.RUnlock()
	sort.SliceStable(recommendations, func(left, right int) bool {
		first, second := recommendations[left], recommendations[right]
		if first.NetBenefitNanos != second.NetBenefitNanos {
			return first.NetBenefitNanos > second.NetBenefitNanos
		}
		if first.PaybackReads != second.PaybackReads {
			return first.PaybackReads < second.PaybackReads
		}
		if first.Candidate.Key != second.Candidate.Key {
			return first.Candidate.Key < second.Candidate.Key
		}
		if first.Candidate.Field != second.Candidate.Field {
			return first.Candidate.Field < second.Candidate.Field
		}
		return first.Candidate.Kind < second.Candidate.Kind
	})
	if limit > 0 && len(recommendations) > limit {
		recommendations = recommendations[:limit]
	}
	return recommendations
}

func sqlDataflowIndexSaturatingAdd(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}
