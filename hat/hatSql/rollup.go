package hatSql

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// TimedMetric is one raw numeric event eligible for automatic time rollups.
type TimedMetric struct {
	Key   string
	At    time.Time
	Value float64
}

// TimeBucket is an aggregate for one key and fixed time interval.
type TimeBucket struct {
	Key        string
	Start, End time.Time
	Count      int
	Sum        float64
	Min        float64
	Max        float64
}

type timeBucketKey struct {
	key   string
	start int64
}

// TimeBucketRollup maintains fixed-width aggregate buckets as events arrive.
type TimeBucketRollup struct {
	mu      sync.RWMutex
	width   time.Duration
	buckets map[timeBucketKey]TimeBucket
}

func NewTimeBucketRollup(width time.Duration) (*TimeBucketRollup, error) {
	if width <= 0 {
		return nil, fmt.Errorf("time bucket width must be positive")
	}
	return &TimeBucketRollup{width: width, buckets: make(map[timeBucketKey]TimeBucket)}, nil
}

// Add automatically includes one event in its keyed time bucket.
func (rollup *TimeBucketRollup) Add(event TimedMetric) error {
	if rollup == nil {
		return fmt.Errorf("time bucket rollup is nil")
	}
	if err := event.validate(); err != nil {
		return err
	}
	rollup.mu.Lock()
	defer rollup.mu.Unlock()
	rollup.addLocked(event)
	return nil
}

// Buckets returns complete or partial aggregate buckets in [start, end), sorted
// first by time and then by key.
func (rollup *TimeBucketRollup) Buckets(start, end time.Time) ([]TimeBucket, error) {
	if rollup == nil {
		return nil, fmt.Errorf("time bucket rollup is nil")
	}
	if end.Before(start) {
		return nil, fmt.Errorf("time bucket range end precedes start")
	}
	rollup.mu.RLock()
	defer rollup.mu.RUnlock()
	result := make([]TimeBucket, 0)
	for _, bucket := range rollup.buckets {
		if !bucket.Start.Before(start) && bucket.Start.Before(end) {
			result = append(result, bucket)
		}
	}
	sort.Slice(result, func(left, right int) bool {
		if !result[left].Start.Equal(result[right].Start) {
			return result[left].Start.Before(result[right].Start)
		}
		return result[left].Key < result[right].Key
	})
	return result, nil
}

// VerifyThrough proves that all raw events before cutoff match the stored
// complete buckets. cutoff must be a bucket boundary so no retained event can
// share a bucket with discarded raw data.
func (rollup *TimeBucketRollup) VerifyThrough(raw []TimedMetric, cutoff time.Time) error {
	if rollup == nil {
		return fmt.Errorf("time bucket rollup is nil")
	}
	if !cutoff.Equal(rollup.bucketStart(cutoff)) {
		return fmt.Errorf("rollup verification cutoff must be a bucket boundary")
	}
	expected := make(map[timeBucketKey]TimeBucket)
	for _, event := range raw {
		if err := event.validate(); err != nil {
			return err
		}
		if event.At.Before(cutoff) {
			rollup.addTo(expected, event)
		}
	}
	rollup.mu.RLock()
	defer rollup.mu.RUnlock()
	actual := make(map[timeBucketKey]TimeBucket)
	for key, bucket := range rollup.buckets {
		if bucket.Start.Before(cutoff) {
			actual[key] = bucket
		}
	}
	if len(actual) != len(expected) {
		return fmt.Errorf("rollup verification bucket count mismatch: expected %d, got %d", len(expected), len(actual))
	}
	for key, want := range expected {
		got, ok := actual[key]
		if !ok || !equalTimeBucket(got, want) {
			return fmt.Errorf("rollup verification mismatch for key %q at %s", key.key, want.Start.Format(time.RFC3339Nano))
		}
	}
	return nil
}

// RetainRawAfterVerified returns only raw events at or after cutoff after a
// successful rollup verification. It never discards data on a failed check.
func (rollup *TimeBucketRollup) RetainRawAfterVerified(raw []TimedMetric, cutoff time.Time) ([]TimedMetric, int, error) {
	if err := rollup.VerifyThrough(raw, cutoff); err != nil {
		return nil, 0, err
	}
	kept := make([]TimedMetric, 0, len(raw))
	removed := 0
	for _, event := range raw {
		if event.At.Before(cutoff) {
			removed++
			continue
		}
		kept = append(kept, event)
	}
	return kept, removed, nil
}

func (event TimedMetric) validate() error {
	if event.At.IsZero() {
		return fmt.Errorf("timed metric timestamp cannot be zero")
	}
	if math.IsNaN(event.Value) || math.IsInf(event.Value, 0) {
		return fmt.Errorf("timed metric value must be finite")
	}
	return nil
}

func (rollup *TimeBucketRollup) addLocked(event TimedMetric) {
	rollup.addTo(rollup.buckets, event)
}

func (rollup *TimeBucketRollup) addTo(buckets map[timeBucketKey]TimeBucket, event TimedMetric) {
	start := rollup.bucketStart(event.At)
	key := timeBucketKey{key: event.Key, start: start.UnixNano()}
	bucket, ok := buckets[key]
	if !ok {
		buckets[key] = TimeBucket{Key: event.Key, Start: start, End: start.Add(rollup.width), Count: 1, Sum: event.Value, Min: event.Value, Max: event.Value}
		return
	}
	bucket.Count++
	bucket.Sum += event.Value
	if event.Value < bucket.Min {
		bucket.Min = event.Value
	}
	if event.Value > bucket.Max {
		bucket.Max = event.Value
	}
	buckets[key] = bucket
}

func (rollup *TimeBucketRollup) bucketStart(at time.Time) time.Time {
	return at.UTC().Truncate(rollup.width)
}

func equalTimeBucket(left, right TimeBucket) bool {
	return left.Key == right.Key && left.Start.Equal(right.Start) && left.End.Equal(right.End) && left.Count == right.Count && equalRollupFloat(left.Sum, right.Sum) && equalRollupFloat(left.Min, right.Min) && equalRollupFloat(left.Max, right.Max)
}

func equalRollupFloat(left, right float64) bool {
	return math.Abs(left-right) <= 1e-12*math.Max(1, math.Max(math.Abs(left), math.Abs(right)))
}
