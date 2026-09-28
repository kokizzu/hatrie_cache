package hatDataStructure

import (
	"testing"
	"time"
)

var t249StatsSink int

func BenchmarkT249ManualQueueStats(b *testing.B) {
	queue := t249StatsFixture()
	now := time.Unix(1_700_000_000, 0).UTC()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		pending := queue.Len()
		dead := queue.DeadLetterLen()
		readyAt, _ := queue.NextReadyAt()
		deadItems := queue.DeadLetters()
		maxAttempts := uint(0)
		for _, item := range deadItems {
			if item.Attempts > maxAttempts {
				maxAttempts = item.Attempts
			}
		}
		readyFlag := 0
		if readyAt.Before(now) {
			readyFlag = 1
		}
		t249StatsSink = pending + dead + len(deadItems) + int(maxAttempts) + readyFlag
	}
}

func BenchmarkT249QueueStats(b *testing.B) {
	queue := t249StatsFixture()
	now := time.Unix(1_700_000_000, 0).UTC()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stats := queue.Stats(now)
		ready := 0
		if stats.Ready {
			ready = 1
		}
		t249StatsSink = stats.Pending + ready + stats.PendingCapacity +
			stats.DeadLetters + stats.DeadLetterLimit + int(stats.MaxAttempts)
	}
}

func t249StatsFixture() *DeadLetterQueue[int] {
	now := time.Unix(1_700_000_000, 0).UTC()
	queue := NewDeadLetterQueue[int](4096, 128)
	for index := 0; index < 4096; index++ {
		queue.EnqueueAt(now.Add(-time.Duration(index+1)*time.Second), index)
	}
	for index := 0; index < 128; index++ {
		item, ok := queue.Pop()
		if !ok {
			break
		}
		queue.FailAt(item, now.Add(-time.Duration(index+1)*time.Second), uint(index+1), "retry")
	}
	return queue
}
