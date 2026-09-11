package hatStorage_test

import (
	"context"
	"strconv"
	"testing"

	"hatrie_cache/hat/hatStorage"
)

var compactionSchedulerStatsSink hatStorage.CompactionSchedulerStats

func BenchmarkCompactionSchedulerStats(b *testing.B) {
	scheduler, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{MaxConcurrent: 4})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 64; index++ {
		if _, err := scheduler.Schedule(strconv.Itoa(index), func(context.Context) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		compactionSchedulerStatsSink = scheduler.Stats()
	}
}
