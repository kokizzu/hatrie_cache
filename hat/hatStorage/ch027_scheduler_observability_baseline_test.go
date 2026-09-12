package hatStorage_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatStorage"
)

var ch027SchedulerStatsSink hatStorage.CompactionSchedulerStats

func BenchmarkCH027SchedulerStatsBaseline(b *testing.B) {
	scheduler, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{MaxConcurrent: 4})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 64; index++ {
		if _, err := scheduler.Schedule("ch027:pending:"+string(rune(index)), func(context.Context) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		ch027SchedulerStatsSink = scheduler.Stats()
	}
}

func BenchmarkCH027SchedulerStats(b *testing.B) {
	scheduler, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{MaxConcurrent: 4})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 64; index++ {
		if _, err := scheduler.Schedule("ch027:pending:"+string(rune(index)), func(context.Context) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		ch027SchedulerStatsSink = scheduler.Stats()
	}
}
