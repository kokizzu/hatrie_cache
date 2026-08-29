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
	options AdaptivePlannerOptions
	shards  [adaptivePlannerShardCount]adaptivePlannerShard
}

const adaptivePlannerShardCount = 32

type adaptivePlannerShard struct {
	mu    sync.RWMutex
	stats map[string]adaptivePlannerStats
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
	planner := &AdaptivePlanner{options: options}
	for index := range planner.shards {
		planner.shards[index].stats = make(map[string]adaptivePlannerStats)
	}
	return planner
}

// ShouldUseIndex reports whether a previously observed predicate remains safe
// to use as an index probe.
func (planner *AdaptivePlanner) ShouldUseIndex(key string) bool {
	if planner == nil {
		return true
	}
	shard := planner.shard(key)
	shard.mu.RLock()
	stats := shard.stats[key]
	shard.mu.RUnlock()
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
	shard := planner.shard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	stats := shard.stats[key]
	stats.samples++
	stats.estimated += uint64(estimated)
	stats.actual += uint64(actual)
	shard.stats[key] = stats
}

func (planner *AdaptivePlanner) shard(key string) *adaptivePlannerShard {
	var hash uint64 = 14695981039346656037
	for index := 0; index < len(key); index++ {
		hash ^= uint64(key[index])
		hash *= 1099511628211
	}
	return &planner.shards[hash&(adaptivePlannerShardCount-1)]
}
