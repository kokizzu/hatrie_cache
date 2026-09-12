package hatStorage

import "time"

// CompactionSchedulerStats is a point-in-time view of queued and completed
// compaction callback attempts. Scheduled, Completed, and Failed count task
// executions, not duplicate Schedule calls that were coalesced.
type CompactionSchedulerStats struct {
	MaxConcurrent int    `json:"max_concurrent"`
	Pending       int    `json:"pending"`
	Running       int    `json:"running"`
	Scheduled     uint64 `json:"scheduled"`
	Completed     uint64 `json:"completed"`
	Failed        uint64 `json:"failed"`
}

// CompactionSchedulerAgeStats is a point-in-time view of the oldest queued
// and running compaction timestamps. Callers can calculate age using the
// supplied clock without making the scheduler read the wall clock internally.
type CompactionSchedulerAgeStats struct {
	OldestPendingAtUnixNanoseconds int64 `json:"oldest_pending_at_unix_ns,omitempty"`
	OldestRunningAtUnixNanoseconds int64 `json:"oldest_running_at_unix_ns,omitempty"`
}

// Stats returns a read-only scheduler snapshot without changing queue state.
// A nil scheduler returns the zero value.
func (scheduler *CompactionScheduler) Stats() CompactionSchedulerStats {
	if scheduler == nil {
		return CompactionSchedulerStats{}
	}
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	return CompactionSchedulerStats{
		MaxConcurrent: scheduler.maxConcurrent,
		Pending:       len(scheduler.pending),
		Running:       len(scheduler.running),
		Scheduled:     scheduler.scheduled,
		Completed:     scheduler.completed,
		Failed:        scheduler.failed,
	}
}

// Ages returns the oldest queued and running timestamps without reading the
// wall clock or changing scheduler state. A nil scheduler returns zero ages.
func (scheduler *CompactionScheduler) Ages() CompactionSchedulerAgeStats {
	if scheduler == nil {
		return CompactionSchedulerAgeStats{}
	}
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	stats := CompactionSchedulerAgeStats{}
	if !scheduler.oldestPending.IsZero() {
		stats.OldestPendingAtUnixNanoseconds = scheduler.oldestPending.UnixNano()
	}
	if !scheduler.oldestRunning.IsZero() {
		stats.OldestRunningAtUnixNanoseconds = scheduler.oldestRunning.UnixNano()
	}
	return stats
}

// OldestPendingAge returns the queued age at now without taking another
// scheduler lock or reading the wall clock internally.
func (stats CompactionSchedulerAgeStats) OldestPendingAge(now time.Time) time.Duration {
	return schedulerStatsAge(stats.OldestPendingAtUnixNanoseconds, now)
}

// OldestRunningAge returns the running age at now without taking another
// scheduler lock or reading the wall clock internally.
func (stats CompactionSchedulerAgeStats) OldestRunningAge(now time.Time) time.Duration {
	return schedulerStatsAge(stats.OldestRunningAtUnixNanoseconds, now)
}

func schedulerStatsAge(timestamp int64, now time.Time) time.Duration {
	if timestamp == 0 {
		return 0
	}
	nowUnixNano := now.UnixNano()
	if nowUnixNano <= timestamp {
		return 0
	}
	return time.Duration(nowUnixNano - timestamp)
}
