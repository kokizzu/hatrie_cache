package hatCache

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"
)

const (
	// DefaultCompactionDebtSchedulerThresholdBytes is the amount of estimated
	// rewritten data that makes one compaction eligible.
	DefaultCompactionDebtSchedulerThresholdBytes int64 = 64 << 20
	// DefaultCompactionDebtSchedulerCheckInterval controls opt-in polling.
	DefaultCompactionDebtSchedulerCheckInterval = time.Second
)

var (
	ErrCompactionDebtSchedulerNil             = errors.New("hatriecache: compaction debt scheduler is nil")
	ErrCompactionDebtSchedulerCompactRequired = errors.New("hatriecache: compaction debt scheduler compact callback is required")
	ErrCompactionDebtSchedulerThreshold       = errors.New("hatriecache: compaction debt scheduler threshold must be positive")
	ErrCompactionDebtSchedulerInterval        = errors.New("hatriecache: compaction debt scheduler interval must be positive")
	ErrCompactionDebtSchedulerDebtInvalid     = errors.New("hatriecache: compaction debt must be non-negative")
	ErrCompactionDebtSchedulerDebtOverflow    = errors.New("hatriecache: compaction debt overflow")
	ErrCompactionDebtSchedulerCompaction      = errors.New("hatriecache: scheduled compaction failed")
	ErrCompactionDebtSchedulerContext         = errors.New("hatriecache: compaction debt scheduler context is nil")
)

// CompactionDebtSchedulerOptions configures one opt-in compaction scheduler.
// The caller supplies estimated bytes written since the previous compaction;
// the scheduler does not inspect or mutate storage unless CompactIfDue or Run
// invokes the callback.
type CompactionDebtSchedulerOptions struct {
	ThresholdBytes int64
	CheckInterval  time.Duration
	CompactOptions LevelDBCompactionOptions
	Compact        func(LevelDBCompactionOptions) (LevelDBCompactionResult, error)
}

// CompactionDebtSchedulerStats is an owned snapshot of scheduler state.
type CompactionDebtSchedulerStats struct {
	DebtBytes         int64
	ThresholdBytes    int64
	CompactionRunning bool
}

// CompactionDebtScheduler accumulates estimated rewrite debt and runs a
// caller-supplied compaction only after the configured threshold is reached.
// It has no global registration and no background goroutine until Run is
// explicitly called.
type CompactionDebtScheduler struct {
	debtBytes         atomic.Int64
	compactionRunning atomic.Bool

	thresholdBytes int64
	checkInterval  time.Duration
	compactOptions LevelDBCompactionOptions
	compact        func(LevelDBCompactionOptions) (LevelDBCompactionResult, error)
}

// NewCompactionDebtScheduler validates options and creates an idle scheduler.
func NewCompactionDebtScheduler(options CompactionDebtSchedulerOptions) (*CompactionDebtScheduler, error) {
	if options.ThresholdBytes < 0 {
		return nil, ErrCompactionDebtSchedulerThreshold
	}
	if options.CheckInterval < 0 {
		return nil, ErrCompactionDebtSchedulerInterval
	}
	if options.Compact == nil {
		return nil, ErrCompactionDebtSchedulerCompactRequired
	}
	if options.ThresholdBytes == 0 {
		options.ThresholdBytes = DefaultCompactionDebtSchedulerThresholdBytes
	}
	if options.CheckInterval == 0 {
		options.CheckInterval = DefaultCompactionDebtSchedulerCheckInterval
	}
	return &CompactionDebtScheduler{
		thresholdBytes: options.ThresholdBytes,
		checkInterval:  options.CheckInterval,
		compactOptions: options.CompactOptions,
		compact:        options.Compact,
	}, nil
}

// AddDebt records estimated bytes written since the last successful
// compaction. It does not start work and is safe to call concurrently.
func (scheduler *CompactionDebtScheduler) AddDebt(bytes int64) error {
	if scheduler == nil {
		return ErrCompactionDebtSchedulerNil
	}
	if bytes < 0 {
		return ErrCompactionDebtSchedulerDebtInvalid
	}
	if bytes == 0 {
		return nil
	}
	for {
		current := scheduler.debtBytes.Load()
		if current > math.MaxInt64-bytes {
			return ErrCompactionDebtSchedulerDebtOverflow
		}
		if scheduler.debtBytes.CompareAndSwap(current, current+bytes) {
			return nil
		}
	}
}

// DebtBytes returns the current un-compacted estimated bytes.
func (scheduler *CompactionDebtScheduler) DebtBytes() int64 {
	if scheduler == nil {
		return 0
	}
	return scheduler.debtBytes.Load()
}

// Stats returns an independently owned scheduler snapshot.
func (scheduler *CompactionDebtScheduler) Stats() CompactionDebtSchedulerStats {
	if scheduler == nil {
		return CompactionDebtSchedulerStats{}
	}
	stats := CompactionDebtSchedulerStats{
		DebtBytes:         scheduler.debtBytes.Load(),
		ThresholdBytes:    scheduler.thresholdBytes,
		CompactionRunning: scheduler.compactionRunning.Load(),
	}
	return stats
}

// CompactIfDue claims the current debt and runs one compaction when the
// threshold is reached. New debt added while the callback runs remains for a
// later cycle. Failed compactions retain their claimed debt for retry.
func (scheduler *CompactionDebtScheduler) CompactIfDue() (LevelDBCompactionResult, bool, error) {
	if scheduler == nil {
		return LevelDBCompactionResult{}, false, ErrCompactionDebtSchedulerNil
	}
	if scheduler.compactionRunning.Load() || scheduler.debtBytes.Load() < scheduler.thresholdBytes {
		return LevelDBCompactionResult{}, false, nil
	}
	if !scheduler.compactionRunning.CompareAndSwap(false, true) {
		return LevelDBCompactionResult{}, false, nil
	}
	claimedDebt := scheduler.debtBytes.Load()
	options := scheduler.compactOptions

	result, err := scheduler.compact(options)
	if err != nil {
		scheduler.compactionRunning.Store(false)
		return LevelDBCompactionResult{}, true, fmt.Errorf("%w: %v", ErrCompactionDebtSchedulerCompaction, err)
	}
	for {
		current := scheduler.debtBytes.Load()
		next := current - claimedDebt
		if next < 0 {
			next = 0
		}
		if scheduler.debtBytes.CompareAndSwap(current, next) {
			break
		}
	}
	scheduler.compactionRunning.Store(false)
	return result, true, nil
}

// Run polls for due work until ctx is canceled or a compaction fails. It is
// deliberately blocking so ownership of the goroutine stays with the caller.
func (scheduler *CompactionDebtScheduler) Run(ctx context.Context) error {
	if scheduler == nil {
		return ErrCompactionDebtSchedulerNil
	}
	if ctx == nil {
		return ErrCompactionDebtSchedulerContext
	}
	ticker := time.NewTicker(scheduler.checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if _, _, err := scheduler.CompactIfDue(); err != nil {
				return err
			}
		}
	}
}
