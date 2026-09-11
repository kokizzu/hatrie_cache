package hatStorage

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
