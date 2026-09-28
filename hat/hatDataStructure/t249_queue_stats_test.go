package hatDataStructure

import (
	"testing"
	"time"
)

func TestDeadLetterQueueStatsReportsCapacityAgeRetriesAndReadiness(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	queue := NewDeadLetterQueue[int](8, 4)
	queue.EnqueueAt(now.Add(-3*time.Second), 1)
	queue.EnqueueAt(now.Add(-1*time.Second), 2)
	queue.EnqueueAt(now.Add(2*time.Second), 3)

	item, ok := queue.Pop()
	if !ok {
		t.Fatal("expected a ready item")
	}
	queue.FailAt(item, now.Add(-4*time.Second), 3, "retry")

	stats := queue.Stats(now)
	if stats.Pending != 2 {
		t.Fatalf("pending = %d, want 2", stats.Pending)
	}
	if !stats.Ready {
		t.Fatal("ready = false, want true")
	}
	if stats.PendingCapacity < 3 {
		t.Fatalf("pending capacity = %d, want at least 3", stats.PendingCapacity)
	}
	if stats.DeadLetters != 1 {
		t.Fatalf("dead letters = %d, want 1", stats.DeadLetters)
	}
	if stats.DeadLetterLimit != 4 {
		t.Fatalf("dead-letter limit = %d, want 4", stats.DeadLetterLimit)
	}
	if stats.PendingAge != time.Second {
		t.Fatalf("pending age = %s, want 1s", stats.PendingAge)
	}
	if stats.OldestDeadAge != 4*time.Second {
		t.Fatalf("oldest dead age = %s, want 4s", stats.OldestDeadAge)
	}
	if stats.MaxAttempts != 3 {
		t.Fatalf("max attempts = %d, want 3", stats.MaxAttempts)
	}
	if !stats.OldestPendingAt.Equal(now.Add(-time.Second)) {
		t.Fatalf("oldest pending at = %s, want %s", stats.OldestPendingAt, now.Add(-time.Second))
	}
	if !stats.OldestDeadAt.Equal(now.Add(-4 * time.Second)) {
		t.Fatalf("oldest dead at = %s, want %s", stats.OldestDeadAt, now.Add(-4*time.Second))
	}
}

func TestDeadLetterQueueStatsNilQueueIsZero(t *testing.T) {
	var queue *DeadLetterQueue[int]
	if stats := queue.Stats(time.Unix(1_700_000_000, 0).UTC()); stats != (DeadLetterQueueStats{}) {
		t.Fatalf("nil queue stats = %#v, want zero", stats)
	}
}
