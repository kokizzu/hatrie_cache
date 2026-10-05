package hatStorage

import (
	"errors"
	"math/bits"
	"sync"
	"time"
)

const (
	// DefaultCompactionIOCalibrationMinBytesPerSecond prevents a slow sample
	// from stopping maintenance completely.
	DefaultCompactionIOCalibrationMinBytesPerSecond uint64 = 1 << 20
	// DefaultCompactionIOCalibrationMaxBytesPerSecond bounds an optimistic
	// sample before a caller has measured the real device.
	DefaultCompactionIOCalibrationMaxBytesPerSecond uint64 = 1 << 40
)

var (
	// ErrCompactionIOCalibrationNil reports a call on a nil calibrator.
	ErrCompactionIOCalibrationNil = errors.New("hatriecache: compaction IO calibration is nil")
	// ErrCompactionIOCalibrationOptionsInvalid reports invalid calibration
	// bounds or an initial rate outside those bounds.
	ErrCompactionIOCalibrationOptionsInvalid = errors.New("hatriecache: compaction IO calibration options are invalid")
	// ErrCompactionIOCalibrationObservationInvalid reports an empty or
	// non-positive-duration sample.
	ErrCompactionIOCalibrationObservationInvalid = errors.New("hatriecache: compaction IO calibration observation is invalid")
)

// CompactionIOCalibrationOptions bounds the feedback estimator. A zero bound
// selects the corresponding sane default. InitialBytesPerSecond may be zero,
// which leaves the first valid sample to establish the starting rate.
type CompactionIOCalibrationOptions struct {
	InitialBytesPerSecond uint64
	MinBytesPerSecond     uint64
	MaxBytesPerSecond     uint64
}

// CompactionIOCalibrationStats is a detached estimator snapshot.
type CompactionIOCalibrationStats struct {
	BytesPerSecond             uint64 `json:"bytes_per_second"`
	LastObservedBytesPerSecond uint64 `json:"last_observed_bytes_per_second"`
	SampleCount                uint64 `json:"sample_count"`
	ObservedBytes              uint64 `json:"observed_bytes"`
}

// CompactionIOCalibration learns a bounded device rate from successful
// compaction observations. It has no goroutine or timer; a scheduler reads the
// current rate only when a calibrated scheduler is explicitly configured.
type CompactionIOCalibration struct {
	mu      sync.Mutex
	initial uint64
	min     uint64
	max     uint64
	stats   CompactionIOCalibrationStats
}

// NewCompactionIOCalibration creates an opt-in bounded EWMA estimator.
func NewCompactionIOCalibration(options CompactionIOCalibrationOptions) (*CompactionIOCalibration, error) {
	min := options.MinBytesPerSecond
	if min == 0 {
		min = DefaultCompactionIOCalibrationMinBytesPerSecond
	}
	max := options.MaxBytesPerSecond
	if max == 0 {
		max = DefaultCompactionIOCalibrationMaxBytesPerSecond
	}
	if min == 0 || max < min || options.InitialBytesPerSecond > max || (options.InitialBytesPerSecond != 0 && options.InitialBytesPerSecond < min) {
		return nil, ErrCompactionIOCalibrationOptionsInvalid
	}
	initial := options.InitialBytesPerSecond
	return &CompactionIOCalibration{
		initial: initial,
		min:     min,
		max:     max,
		stats:   CompactionIOCalibrationStats{BytesPerSecond: initial},
	}, nil
}

// Observe records one successful compaction sample and returns the smoothed
// bytes-per-second recommendation. The sample is clamped before it enters the
// EWMA, so one anomalous callback cannot create an unbounded wait or burst.
func (calibration *CompactionIOCalibration) Observe(bytes uint64, duration time.Duration) (uint64, error) {
	if calibration == nil {
		return 0, ErrCompactionIOCalibrationNil
	}
	if bytes == 0 || duration <= 0 {
		return calibration.BytesPerSecond(), ErrCompactionIOCalibrationObservationInvalid
	}
	observed := clampCompactionIORate(compactionObservedBytesPerSecond(bytes, duration), calibration.min, calibration.max)
	calibration.mu.Lock()
	defer calibration.mu.Unlock()
	calibration.stats.LastObservedBytesPerSecond = observed
	calibration.stats.SampleCount = saturatingCompactionAdd(calibration.stats.SampleCount, 1)
	calibration.stats.ObservedBytes = saturatingCompactionAdd(calibration.stats.ObservedBytes, bytes)
	if calibration.stats.BytesPerSecond == 0 {
		calibration.stats.BytesPerSecond = observed
	} else {
		calibration.stats.BytesPerSecond = smoothCompactionIORate(calibration.stats.BytesPerSecond, observed)
	}
	return calibration.stats.BytesPerSecond, nil
}

// BytesPerSecond returns the current bounded recommendation without changing
// estimator state.
func (calibration *CompactionIOCalibration) BytesPerSecond() uint64 {
	if calibration == nil {
		return 0
	}
	calibration.mu.Lock()
	defer calibration.mu.Unlock()
	return calibration.stats.BytesPerSecond
}

// Snapshot returns a detached estimator snapshot.
func (calibration *CompactionIOCalibration) Snapshot() CompactionIOCalibrationStats {
	if calibration == nil {
		return CompactionIOCalibrationStats{}
	}
	calibration.mu.Lock()
	defer calibration.mu.Unlock()
	return calibration.stats
}

// Reset removes observations and restores the optional initial rate.
func (calibration *CompactionIOCalibration) Reset() {
	if calibration == nil {
		return
	}
	calibration.mu.Lock()
	calibration.stats = CompactionIOCalibrationStats{BytesPerSecond: calibration.initial}
	calibration.mu.Unlock()
}

func compactionObservedBytesPerSecond(bytes uint64, duration time.Duration) uint64 {
	denominator := uint64(duration)
	if denominator == 0 {
		return ^uint64(0)
	}
	hi, lo := bits.Mul64(bytes, uint64(time.Second))
	if hi >= denominator {
		return ^uint64(0)
	}
	rate, _ := bits.Div64(hi, lo, denominator)
	return rate
}

func clampCompactionIORate(rate, min, max uint64) uint64 {
	if rate < min {
		return min
	}
	if rate > max {
		return max
	}
	return rate
}

func smoothCompactionIORate(current, observed uint64) uint64 {
	if observed >= current {
		delta := observed - current
		return current + compactionIORoundedQuarter(delta)
	}
	delta := current - observed
	return current - compactionIORoundedQuarter(delta)
}

func compactionIORoundedQuarter(delta uint64) uint64 {
	quarter := delta / 4
	if delta%4 != 0 {
		quarter++
	}
	return quarter
}
