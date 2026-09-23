package hatReplication

import (
	"strings"
	"sync"
	"time"

	"hatrie_cache/hat/hatMetrics"
)

var targetLatencyMillisBuckets = []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000}
var targetBatchItemsBuckets = []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 4096, 16384}
var retryDelayMillisBuckets = []float64{1, 10, 50, 100, 250, 500, 1000, 5000, 30000, 60000}
var queueTimingMillisBuckets = []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000}

// HistogramSnapshot is the stable histogram representation used by
// replication metrics.
type HistogramSnapshot = hatMetrics.HistogramSnapshot

// ApplyMetricsSnapshot reports acknowledgement-observed replication apply
// work for one target. Rates use the elapsed time between the first and most
// recent accepted sequence observation.
type ApplyMetricsSnapshot struct {
	LastAppliedSequence          uint64  `json:"last_applied_sequence,omitempty"`
	AppliedBatches               uint64  `json:"applied_batches"`
	AppliedEntries               uint64  `json:"applied_entries"`
	AppliedPayloadBytes          uint64  `json:"applied_payload_bytes"`
	AppliedEntriesPerSecond      float64 `json:"applied_entries_per_second,omitempty"`
	AppliedPayloadBytesPerSecond float64 `json:"applied_payload_bytes_per_second,omitempty"`
}

// MetricsSnapshot is an immutable copy of replication transport metrics.
type MetricsSnapshot struct {
	TargetLatencyMillis       map[string]HistogramSnapshot
	TargetBatchItems          map[string]HistogramSnapshot
	TargetWireBytes           map[string]map[string]uint64
	TargetWireRequests        map[string]map[string]uint64
	TargetApply               map[string]ApplyMetricsSnapshot
	RetryDelayMillis          HistogramSnapshot
	QueueWaitMillis           HistogramSnapshot
	QueueServiceMillis        HistogramSnapshot
	CircuitBreakerTransitions map[string]map[string]uint64
}

// Metrics records replication transport observations independently of the
// cache server or a specific replication client implementation.
type Metrics struct {
	mu                 sync.Mutex
	targetLatency      map[string]*hatMetrics.Histogram
	targetBatchItems   map[string]*hatMetrics.Histogram
	targetWireBytes    map[string]map[string]uint64
	targetWireRequests map[string]map[string]uint64
	targetApply        map[string]*targetApplyMetrics
	retryDelayMillis   *hatMetrics.Histogram
	queueWaitMillis    *hatMetrics.Histogram
	queueServiceMillis *hatMetrics.Histogram
	breakerTransitions map[string]map[string]uint64
}

type targetApplyMetrics struct {
	lastAppliedSequence uint64
	appliedBatches      uint64
	appliedEntries      uint64
	appliedPayloadBytes uint64
	firstAt             time.Time
	lastAt              time.Time
}

// ObserveTargetLatency records one completed transport attempt.
func (metrics *Metrics) ObserveTargetLatency(target string, duration time.Duration) {
	if metrics == nil || duration < 0 {
		return
	}
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	metrics.targetHistogramLocked(&metrics.targetLatency, target, targetLatencyMillisBuckets).Observe(float64(duration) / float64(time.Millisecond))
}

// ObserveTargetBatchItems records one delivered replication batch size.
func (metrics *Metrics) ObserveTargetBatchItems(target string, items int) {
	if metrics == nil || items <= 0 {
		return
	}
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	metrics.targetHistogramLocked(&metrics.targetBatchItems, target, targetBatchItemsBuckets).Observe(float64(items))
}

// ObserveTargetWireBytes records bytes sent for one request, grouped by
// target and content encoding. An empty encoding is recorded as identity.
func (metrics *Metrics) ObserveTargetWireBytes(target, encoding string, bytes uint64) {
	if metrics == nil || strings.TrimSpace(target) == "" {
		return
	}
	target = strings.TrimSpace(target)
	encoding = strings.ToLower(strings.TrimSpace(encoding))
	if encoding == "" {
		encoding = "identity"
	}
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	if metrics.targetWireBytes == nil {
		metrics.targetWireBytes = map[string]map[string]uint64{}
	}
	if metrics.targetWireBytes[target] == nil {
		metrics.targetWireBytes[target] = map[string]uint64{}
	}
	metrics.targetWireBytes[target][encoding] += bytes
	if metrics.targetWireRequests == nil {
		metrics.targetWireRequests = map[string]map[string]uint64{}
	}
	if metrics.targetWireRequests[target] == nil {
		metrics.targetWireRequests[target] = map[string]uint64{}
	}
	metrics.targetWireRequests[target][encoding]++
}

// ObserveTargetApply records one successfully acknowledged target apply. A
// sequence is accepted at most once per target, so retries and out-of-order
// completions cannot inflate throughput counters.
func (metrics *Metrics) ObserveTargetApply(target string, sequence, entries, payloadBytes uint64, at time.Time) {
	if metrics == nil || strings.TrimSpace(target) == "" || sequence == 0 || entries == 0 {
		return
	}
	target = strings.TrimSpace(target)
	if at.IsZero() {
		at = time.Now()
	}
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	if metrics.targetApply == nil {
		metrics.targetApply = map[string]*targetApplyMetrics{}
	}
	apply := metrics.targetApply[target]
	if apply == nil {
		apply = &targetApplyMetrics{}
		metrics.targetApply[target] = apply
	}
	if sequence <= apply.lastAppliedSequence {
		return
	}
	if apply.appliedBatches == 0 {
		apply.firstAt = at
	}
	apply.lastAppliedSequence = sequence
	apply.appliedBatches++
	apply.appliedEntries += entries
	apply.appliedPayloadBytes += payloadBytes
	if at.After(apply.lastAt) {
		apply.lastAt = at
	}
}

// ObserveRetryDelay records one asynchronous retry delay.
func (metrics *Metrics) ObserveRetryDelay(duration time.Duration) {
	if metrics == nil || duration < 0 {
		return
	}
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	if metrics.retryDelayMillis == nil {
		metrics.retryDelayMillis = hatMetrics.NewHistogram(retryDelayMillisBuckets)
	}
	metrics.retryDelayMillis.Observe(float64(duration) / float64(time.Millisecond))
}

// ObserveQueueWait records the time a job spent resident before delivery.
func (metrics *Metrics) ObserveQueueWait(duration time.Duration) {
	if metrics == nil || duration < 0 {
		return
	}
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	if metrics.queueWaitMillis == nil {
		metrics.queueWaitMillis = hatMetrics.NewHistogram(queueTimingMillisBuckets)
	}
	metrics.queueWaitMillis.Observe(float64(duration) / float64(time.Millisecond))
}

// ObserveQueueService records the duration of one asynchronous job delivery.
func (metrics *Metrics) ObserveQueueService(duration time.Duration) {
	if metrics == nil || duration < 0 {
		return
	}
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	if metrics.queueServiceMillis == nil {
		metrics.queueServiceMillis = hatMetrics.NewHistogram(queueTimingMillisBuckets)
	}
	metrics.queueServiceMillis.Observe(float64(duration) / float64(time.Millisecond))
}

// RecordCircuitTransition records one target circuit-breaker state change.
func (metrics *Metrics) RecordCircuitTransition(target, state string) {
	if metrics == nil || target == "" || state == "" {
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

// Snapshot returns an independent point-in-time metrics copy.
func (metrics *Metrics) Snapshot() MetricsSnapshot {
	if metrics == nil {
		return MetricsSnapshot{}
	}
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	var targetApply map[string]ApplyMetricsSnapshot
	if len(metrics.targetApply) > 0 {
		targetApply = snapshotTargetApply(metrics.targetApply)
	}
	return MetricsSnapshot{
		TargetLatencyMillis:       snapshotHistogramMap(metrics.targetLatency),
		TargetBatchItems:          snapshotHistogramMap(metrics.targetBatchItems),
		TargetWireBytes:           snapshotCounterMap(metrics.targetWireBytes),
		TargetWireRequests:        snapshotCounterMap(metrics.targetWireRequests),
		TargetApply:               targetApply,
		RetryDelayMillis:          snapshotHistogram(metrics.retryDelayMillis),
		QueueWaitMillis:           snapshotHistogram(metrics.queueWaitMillis),
		QueueServiceMillis:        snapshotHistogram(metrics.queueServiceMillis),
		CircuitBreakerTransitions: snapshotTransitions(metrics.breakerTransitions),
	}
}

func snapshotTargetApply(source map[string]*targetApplyMetrics) map[string]ApplyMetricsSnapshot {
	if len(source) == 0 {
		return nil
	}
	out := make(map[string]ApplyMetricsSnapshot, len(source))
	for target, apply := range source {
		if apply == nil {
			continue
		}
		snapshot := ApplyMetricsSnapshot{
			LastAppliedSequence: apply.lastAppliedSequence,
			AppliedBatches:      apply.appliedBatches,
			AppliedEntries:      apply.appliedEntries,
			AppliedPayloadBytes: apply.appliedPayloadBytes,
		}
		if elapsed := apply.lastAt.Sub(apply.firstAt).Seconds(); elapsed > 0 {
			snapshot.AppliedEntriesPerSecond = float64(apply.appliedEntries) / elapsed
			snapshot.AppliedPayloadBytesPerSecond = float64(apply.appliedPayloadBytes) / elapsed
		}
		out[target] = snapshot
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (metrics *Metrics) targetHistogramLocked(targets *map[string]*hatMetrics.Histogram, target string, bounds []float64) *hatMetrics.Histogram {
	if *targets == nil {
		*targets = map[string]*hatMetrics.Histogram{}
	}
	histogram := (*targets)[target]
	if histogram == nil {
		histogram = hatMetrics.NewHistogram(bounds)
		(*targets)[target] = histogram
	}
	return histogram
}

func snapshotHistogram(histogram *hatMetrics.Histogram) HistogramSnapshot {
	if histogram == nil {
		return HistogramSnapshot{}
	}
	return histogram.Snapshot()
}

func snapshotHistogramMap(source map[string]*hatMetrics.Histogram) map[string]HistogramSnapshot {
	if len(source) == 0 {
		return nil
	}
	out := make(map[string]HistogramSnapshot, len(source))
	for target, histogram := range source {
		out[target] = snapshotHistogram(histogram)
	}
	return out
}

func snapshotTransitions(source map[string]map[string]uint64) map[string]map[string]uint64 {
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

func snapshotCounterMap(source map[string]map[string]uint64) map[string]map[string]uint64 {
	if len(source) == 0 {
		return nil
	}
	out := make(map[string]map[string]uint64, len(source))
	for target, counters := range source {
		out[target] = make(map[string]uint64, len(counters))
		for label, value := range counters {
			out[target][label] = value
		}
	}
	return out
}
