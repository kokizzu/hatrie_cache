package hatStorage

import "time"

// CompactionDiagnosticsSummary is an allocation-free aggregate of the
// registered arrangement observations. InputBytes and OutputBytes are
// cumulative saturating counters; CurrentCompactionDebtBytes is the sum of
// each arrangement's latest reported debt gauge.
type CompactionDiagnosticsSummary struct {
	Arrangements               int    `json:"arrangements"`
	TotalObservations          uint64 `json:"total_observations"`
	SuccessfulCompactions      uint64 `json:"successful_compactions"`
	FailedCompactions          uint64 `json:"failed_compactions"`
	InputBytes                 uint64 `json:"input_bytes"`
	OutputBytes                uint64 `json:"output_bytes"`
	CurrentCompactionDebtBytes uint64 `json:"current_compaction_debt_bytes"`
}

// Summary returns bounded aggregate diagnostics without materializing the
// per-arrangement history. A nil registry returns the zero value.
func (diagnostics *CompactionDiagnostics) Summary() CompactionDiagnosticsSummary {
	if diagnostics == nil {
		return CompactionDiagnosticsSummary{}
	}
	diagnostics.mu.RLock()
	defer diagnostics.mu.RUnlock()
	var summary CompactionDiagnosticsSummary
	summary.Arrangements = len(diagnostics.entries)
	for _, entry := range diagnostics.entries {
		summary.TotalObservations = saturatingCompactionDiagnosticsAdd(summary.TotalObservations, entry.totalObservations)
		summary.SuccessfulCompactions = saturatingCompactionDiagnosticsAdd(summary.SuccessfulCompactions, entry.successfulCompactions)
		summary.FailedCompactions = saturatingCompactionDiagnosticsAdd(summary.FailedCompactions, entry.failedCompactions)
		summary.InputBytes = saturatingCompactionDiagnosticsAdd(summary.InputBytes, entry.totalInputBytes)
		summary.OutputBytes = saturatingCompactionDiagnosticsAdd(summary.OutputBytes, entry.totalOutputBytes)
		summary.CurrentCompactionDebtBytes = saturatingCompactionDiagnosticsAdd(summary.CurrentCompactionDebtBytes, entry.currentCompactionDebtBytes)
	}
	return summary
}

// CompactionMetrics is a point-in-time maintenance snapshot. It combines the
// scheduler's backlog and age signals with aggregate compaction byte metrics
// so operators can identify queued work and rewrite amplification in one read.
type CompactionMetrics struct {
	MaxConcurrent        int    `json:"max_concurrent"`
	Pending              int    `json:"pending"`
	Running              int    `json:"running"`
	Scheduled            uint64 `json:"scheduled"`
	Completed            uint64 `json:"completed"`
	Failed               uint64 `json:"failed"`
	IOBytesPerSecond     uint64 `json:"io_bytes_per_second"`
	IOThrottledTaskCount uint64 `json:"io_throttled_task_count"`
	IOThrottledBytes     uint64 `json:"io_throttled_bytes"`
	IOWaitNanoseconds    uint64 `json:"io_wait_nanoseconds"`

	OldestPendingAge time.Duration `json:"oldest_pending_age"`
	OldestRunningAge time.Duration `json:"oldest_running_age"`

	Arrangements               int    `json:"arrangements"`
	TotalObservations          uint64 `json:"total_observations"`
	SuccessfulCompactions      uint64 `json:"successful_compactions"`
	FailedCompactions          uint64 `json:"failed_compactions"`
	InputBytes                 uint64 `json:"input_bytes"`
	OutputBytes                uint64 `json:"output_bytes"`
	CurrentCompactionDebtBytes uint64 `json:"current_compaction_debt_bytes"`
}

// SnapshotCompactionMetrics returns a bounded, allocation-free snapshot from
// the optional scheduler and diagnostics registry. The caller supplies now so
// tests and monitoring scrapes can use a consistent clock without hidden wall
// clock reads.
func SnapshotCompactionMetrics(scheduler *CompactionScheduler, diagnostics *CompactionDiagnostics, now time.Time) CompactionMetrics {
	var metrics CompactionMetrics
	if scheduler != nil {
		scheduler.mu.Lock()
		metrics.MaxConcurrent = scheduler.maxConcurrent
		metrics.Pending = len(scheduler.pending) + len(scheduler.priorityPending)
		metrics.Running = len(scheduler.running)
		metrics.Scheduled = scheduler.scheduled
		metrics.Completed = scheduler.completed
		metrics.Failed = scheduler.failed
		if scheduler.ioState != nil {
			ioStats := scheduler.ioState.throttle.stats()
			metrics.IOBytesPerSecond = ioStats.bytesPerSecond
			metrics.IOThrottledTaskCount = ioStats.throttledTasks
			metrics.IOThrottledBytes = ioStats.throttledBytes
			metrics.IOWaitNanoseconds = ioStats.waitNanoseconds
		}
		if !scheduler.oldestPending.IsZero() {
			metrics.OldestPendingAge = schedulerStatsAge(scheduler.oldestPending.UnixNano(), now)
		}
		if !scheduler.oldestRunning.IsZero() {
			metrics.OldestRunningAge = schedulerStatsAge(scheduler.oldestRunning.UnixNano(), now)
		}
		scheduler.mu.Unlock()
	}
	if diagnostics != nil {
		summary := diagnostics.Summary()
		metrics.Arrangements = summary.Arrangements
		metrics.TotalObservations = summary.TotalObservations
		metrics.SuccessfulCompactions = summary.SuccessfulCompactions
		metrics.FailedCompactions = summary.FailedCompactions
		metrics.InputBytes = summary.InputBytes
		metrics.OutputBytes = summary.OutputBytes
		metrics.CurrentCompactionDebtBytes = summary.CurrentCompactionDebtBytes
	}
	return metrics
}

// InputToOutputRatio reports cumulative compaction input divided by output.
// The ratio is undefined when no output bytes have been recorded.
func (metrics CompactionMetrics) InputToOutputRatio() (float64, bool) {
	if metrics.OutputBytes == 0 {
		return 0, false
	}
	return float64(metrics.InputBytes) / float64(metrics.OutputBytes), true
}
