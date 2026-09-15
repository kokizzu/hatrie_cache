package hatStorage

import (
	"context"
	"strconv"
	"testing"
)

var compactionSchedulerFastPathSink CompactionRun

func BenchmarkCompactionSchedulerRunC207(b *testing.B) {
	for _, taskCount := range []int{1, 4, 64} {
		b.Run("tasks-"+strconv.Itoa(taskCount), func(b *testing.B) {
			scheduler, err := NewCompactionScheduler(CompactionSchedulerOptions{MaxConcurrent: 4})
			if err != nil {
				b.Fatal(err)
			}
			callback := func(context.Context) error { return nil }
			for task := 0; task < taskCount; task++ {
				if _, err := scheduler.Schedule(strconv.Itoa(task), callback); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				compactionSchedulerFastPathSink, err = scheduler.Run(context.Background())
				if err != nil {
					b.Fatal(err)
				}
				for task := 0; task < taskCount; task++ {
					if _, err := scheduler.Schedule(strconv.Itoa(task), callback); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
