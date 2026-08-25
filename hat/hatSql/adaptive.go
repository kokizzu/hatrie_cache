package hatSql

import "sync"

// AdaptivePlannerOptions controls when observed index underestimation causes a
// later query to prefer the established scan path. Zero values are conservative.
type AdaptivePlannerOptions struct {
	MinSamples          int
	UnderestimateFactor int
}

// AdaptivePlanner retains bounded per-predicate selectivity feedback.
type AdaptivePlanner struct {
	mu      sync.Mutex
	options AdaptivePlannerOptions
	stats   map[string]adaptivePlannerStats
}

type adaptivePlannerStats struct {
	samples   int
	estimated uint64
	actual    uint64
}

// NewAdaptivePlanner creates a feedback planner. One sample and a factor of
// four are conservative defaults, avoiding an eager plan switch on noise.
func NewAdaptivePlanner(options AdaptivePlannerOptions) *AdaptivePlanner {
	if options.MinSamples <= 0 {
		options.MinSamples = 1
	}
	if options.UnderestimateFactor <= 1 {
		options.UnderestimateFactor = 4
	}
	return &AdaptivePlanner{options: options, stats: make(map[string]adaptivePlannerStats)}
}

// ShouldUseIndex reports whether a previously observed predicate remains safe
// to use as an index probe.
func (planner *AdaptivePlanner) ShouldUseIndex(key string) bool {
	if planner == nil {
		return true
	}
	planner.mu.Lock()
	defer planner.mu.Unlock()
	stats := planner.stats[key]
	if stats.samples < planner.options.MinSamples || stats.estimated == 0 {
		return true
	}
	return stats.actual <= stats.estimated*uint64(planner.options.UnderestimateFactor)
}

// ObserveIndex records one estimated and actual index candidate cardinality.
func (planner *AdaptivePlanner) ObserveIndex(key string, estimated, actual int) {
	if planner == nil || key == "" || estimated < 0 || actual < 0 {
		return
	}
	planner.mu.Lock()
	defer planner.mu.Unlock()
	stats := planner.stats[key]
	stats.samples++
	stats.estimated += uint64(estimated)
	stats.actual += uint64(actual)
	planner.stats[key] = stats
}
