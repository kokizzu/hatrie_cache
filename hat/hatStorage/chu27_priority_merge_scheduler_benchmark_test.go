//go:build !chu27baseline

package hatStorage_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatStorage"
)

var chu27CompactionSchedulerBenchmarkSink hatStorage.CompactionRun

func BenchmarkCHU27CompactionSchedulerLegacy(b *testing.B) {
	benchmarkCHU27CompactionScheduler(b, false)
}

func BenchmarkCHU27CompactionSchedulerPriority(b *testing.B) {
	benchmarkCHU27CompactionScheduler(b, true)
}

func benchmarkCHU27CompactionScheduler(b *testing.B, prioritized bool) {
	b.Helper()
	const taskCount = 64
	noop := func(context.Context) error { return nil }
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		scheduler, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{MaxConcurrent: 1})
		if err != nil {
			b.Fatal(err)
		}
		for task := 0; task < taskCount; task++ {
			name := "task-" + string(rune('A'+task))
			var queued bool
			if prioritized {
				queued, err = scheduler.ScheduleWithPriority(name, task%8, noop)
			} else {
				queued, err = scheduler.Schedule(name, noop)
			}
			if err != nil || !queued {
				b.Fatalf("schedule task %q = %t/%v", name, queued, err)
			}
		}
		chu27CompactionSchedulerBenchmarkSink, err = scheduler.Run(context.Background())
		if err != nil || chu27CompactionSchedulerBenchmarkSink.Completed != taskCount {
			b.Fatalf("Run() = %#v/%v", chu27CompactionSchedulerBenchmarkSink, err)
		}
	}
}
