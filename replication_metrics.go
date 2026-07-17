package hatriecache

import (
	"sort"
	"strings"
	"sync"
	"time"
)

var replicationTargetLatencyMillisBuckets = []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000}
var replicationBatchItemsBuckets = []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 4096, 16384}
var replicationRetryDelayMillisBuckets = []float64{1, 10, 50, 100, 250, 500, 1000, 5000, 30000, 60000}

type replicationMetrics struct {
	mu                 sync.Mutex
	targetLatency      map[string]*replicationHistogram
	targetBatchItems   map[string]*replicationHistogram
	retryDelayMillis   *replicationHistogram
	breakerTransitions map[string]map[string]uint64
}

type replicationHistogram struct {
	bounds  []float64
	buckets []uint64
	count   uint64
	sum     float64
}

type ReplicationMetricsSnapshot struct {
	TargetLatencyMillis       map[string]ReplicationHistogramSnapshot
	TargetBatchItems          map[string]ReplicationHistogramSnapshot
	RetryDelayMillis          ReplicationHistogramSnapshot
	CircuitBreakerTransitions map[string]map[string]uint64
}

type ReplicationHistogramSnapshot struct {
	Bounds  []float64
	Buckets []uint64
	Count   uint64
	Sum     float64
}

func (replicator *HTTPReplicator) recordReplicationTargetLatency(target TopologyNode, duration time.Duration) {
	if replicator == nil {
		return
	}
	replicator.metrics.observeTargetLatency(replicationMetricsTarget(target), float64(duration)/float64(time.Millisecond))
}

func (replicator *HTTPReplicator) recordReplicationBatchSize(target TopologyNode, items int) {
	if replicator == nil || items <= 0 {
		return
	}
	replicator.metrics.observeTargetBatchItems(replicationMetricsTarget(target), float64(items))
}

func (replicator *HTTPReplicator) recordReplicationRetryDelay(duration time.Duration) {
	if replicator == nil || duration < 0 {
		return
	}
	replicator.metrics.observeRetryDelay(float64(duration) / float64(time.Millisecond))
}

func (replicator *HTTPReplicator) recordReplicationCircuitTransition(target TopologyNode, state string) {
	if replicator == nil || strings.TrimSpace(state) == "" {
		return
	}
	replicator.metrics.recordCircuitTransition(replicationMetricsTarget(target), state)
}

func (replicator *HTTPReplicator) MetricsSnapshot() ReplicationMetricsSnapshot {
	if replicator == nil {
		return ReplicationMetricsSnapshot{}
	}
	return replicator.metrics.snapshot()
}

func (metrics *replicationMetrics) observeTargetLatency(target string, value float64) {
	if metrics == nil {
		return
	}
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	histogram := metrics.targetHistogramLocked(&metrics.targetLatency, target, replicationTargetLatencyMillisBuckets)
	histogram.observe(value)
}

func (metrics *replicationMetrics) observeTargetBatchItems(target string, value float64) {
	if metrics == nil {
		return
	}
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	histogram := metrics.targetHistogramLocked(&metrics.targetBatchItems, target, replicationBatchItemsBuckets)
	histogram.observe(value)
}

func (metrics *replicationMetrics) observeRetryDelay(value float64) {
	if metrics == nil {
		return
	}
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	if metrics.retryDelayMillis == nil {
		metrics.retryDelayMillis = newReplicationHistogram(replicationRetryDelayMillisBuckets)
	}
	metrics.retryDelayMillis.observe(value)
}

func (metrics *replicationMetrics) recordCircuitTransition(target string, state string) {
	if metrics == nil {
		return
	}
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	if metrics.breakerTransitions == nil {
		metrics.breakerTransitions = map[string]map[string]uint64{}
	}
	if metrics.breakerTransitions[target] == nil {
		metrics.breakerTransitions[target] = map[string]uint64{}
	}
	metrics.breakerTransitions[target][state]++
}

func (metrics *replicationMetrics) targetHistogramLocked(targets *map[string]*replicationHistogram, target string, bounds []float64) *replicationHistogram {
	if *targets == nil {
		*targets = map[string]*replicationHistogram{}
	}
	histogram := (*targets)[target]
	if histogram == nil {
		histogram = newReplicationHistogram(bounds)
		(*targets)[target] = histogram
	}
	return histogram
}

func (metrics *replicationMetrics) snapshot() ReplicationMetricsSnapshot {
	if metrics == nil {
		return ReplicationMetricsSnapshot{}
	}
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	return ReplicationMetricsSnapshot{
		TargetLatencyMillis:       snapshotReplicationHistogramMap(metrics.targetLatency),
		TargetBatchItems:          snapshotReplicationHistogramMap(metrics.targetBatchItems),
		RetryDelayMillis:          snapshotReplicationHistogram(metrics.retryDelayMillis),
		CircuitBreakerTransitions: snapshotReplicationTransitionMap(metrics.breakerTransitions),
	}
}

func newReplicationHistogram(bounds []float64) *replicationHistogram {
	copiedBounds := append([]float64(nil), bounds...)
	return &replicationHistogram{
		bounds:  copiedBounds,
		buckets: make([]uint64, len(copiedBounds)),
	}
}

func (histogram *replicationHistogram) observe(value float64) {
	if histogram == nil {
		return
	}
	if value < 0 {
		value = 0
	}
	histogram.count++
	histogram.sum += value
	for idx, bound := range histogram.bounds {
		if value <= bound {
			histogram.buckets[idx]++
			return
		}
	}
}

func snapshotReplicationHistogram(histogram *replicationHistogram) ReplicationHistogramSnapshot {
	if histogram == nil {
		return ReplicationHistogramSnapshot{}
	}
	return ReplicationHistogramSnapshot{
		Bounds:  append([]float64(nil), histogram.bounds...),
		Buckets: append([]uint64(nil), histogram.buckets...),
		Count:   histogram.count,
		Sum:     histogram.sum,
	}
}

func snapshotReplicationHistogramMap(source map[string]*replicationHistogram) map[string]ReplicationHistogramSnapshot {
	if len(source) == 0 {
		return nil
	}
	out := make(map[string]ReplicationHistogramSnapshot, len(source))
	for target, histogram := range source {
		out[target] = snapshotReplicationHistogram(histogram)
	}
	return out
}

func snapshotReplicationTransitionMap(source map[string]map[string]uint64) map[string]map[string]uint64 {
	if len(source) == 0 {
		return nil
	}
	out := make(map[string]map[string]uint64, len(source))
	for target, transitions := range source {
		out[target] = make(map[string]uint64, len(transitions))
		for state, count := range transitions {
			out[target][state] = count
		}
	}
	return out
}

func replicationMetricsTarget(target TopologyNode) string {
	if value := strings.TrimSpace(target.ID); value != "" {
		return value
	}
	if value := strings.TrimSpace(target.Address); value != "" {
		return value
	}
	return "unknown"
}

func sortedReplicationMetricTargets[T any](values map[string]T) []string {
	targets := make([]string, 0, len(values))
	for target := range values {
		targets = append(targets, target)
	}
	sort.Strings(targets)
	return targets
}
