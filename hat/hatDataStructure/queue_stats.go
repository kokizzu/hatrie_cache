package hatDataStructure

import "time"

// DeadLetterQueueStats is a point-in-time operational snapshot. It does not
// retain references to queue values and is safe for callers to serialize.
type DeadLetterQueueStats struct {
	Pending         int           `json:"pending"`
	Ready           bool          `json:"ready"`
	PendingCapacity int           `json:"pending_capacity"`
	DeadLetters     int           `json:"dead_letters"`
	DeadLetterLimit int           `json:"dead_letter_limit"`
	OldestPendingAt time.Time     `json:"oldest_pending_at"`
	NextReadyAt     time.Time     `json:"next_ready_at"`
	PendingAge      time.Duration `json:"pending_age"`
	OldestDeadAt    time.Time     `json:"oldest_dead_at"`
	OldestDeadAge   time.Duration `json:"oldest_dead_age"`
	MaxAttempts     uint          `json:"max_attempts"`
}

// Stats reports queue depth, capacity, readiness, consumer lag, and retained
// failure information without copying pending or dead-letter values.
func (queue *DeadLetterQueue[T]) Stats(now time.Time) DeadLetterQueueStats {
	if queue == nil {
		return DeadLetterQueueStats{}
	}

	stats := DeadLetterQueueStats{
		Pending:         len(queue.pending.items),
		PendingCapacity: cap(queue.pending.items),
		DeadLetters:     len(queue.dead),
		DeadLetterLimit: queue.deadLimit,
	}
	if len(queue.pending.items) > 0 {
		stats.OldestPendingAt = queue.pending.items[0].ReadyAt
		stats.NextReadyAt = stats.OldestPendingAt
		if !stats.OldestPendingAt.After(now) {
			stats.Ready = true
			stats.PendingAge = now.Sub(stats.OldestPendingAt)
		}
	}
	for _, item := range queue.dead {
		if stats.OldestDeadAt.IsZero() || item.FailedAt.Before(stats.OldestDeadAt) {
			stats.OldestDeadAt = item.FailedAt
		}
		if item.Attempts > stats.MaxAttempts {
			stats.MaxAttempts = item.Attempts
		}
	}
	if !stats.OldestDeadAt.IsZero() && !stats.OldestDeadAt.After(now) {
		stats.OldestDeadAge = now.Sub(stats.OldestDeadAt)
	}
	return stats
}
