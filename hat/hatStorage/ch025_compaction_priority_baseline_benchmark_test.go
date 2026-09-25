package hatStorage_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"hatrie_cache/hat/hatStorage"
)

var ch025BaselineCompactionRunSink hatStorage.CompactionRun
var ch025PolicyCompactionRunSink hatStorage.CompactionRun

func BenchmarkCH025BaselineExplicitPrioritySchedule(b *testing.B) {
	noop := func(context.Context) error { return nil }
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		scheduler, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{MaxConcurrent: 1})
		if err != nil {
			b.Fatal(err)
		}
		for task := 0; task < 64; task++ {
			if _, err := scheduler.ScheduleWithPriority(ch025TaskName(task), task+1, noop); err != nil {
				b.Fatal(err)
			}
		}
		ch025BaselineCompactionRunSink, err = scheduler.Run(context.Background())
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCH025PolicyPrioritySchedule(b *testing.B) {
	policy := hatStorage.DefaultCompactionPriorityPolicy()
	noop := func(context.Context) error { return nil }
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		scheduler, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{
			MaxConcurrent:  1,
			PriorityPolicy: &policy,
		})
		if err != nil {
			b.Fatal(err)
		}
		for task := 0; task < 64; task++ {
			if _, err := scheduler.ScheduleWithPriorityMetrics(ch025TaskName(task), hatStorage.CompactionPriorityMetrics{
				FreshnessLag:     time.Duration(task+1) * time.Minute,
				ReclaimableBytes: uint64(task+1) << 20,
			}, noop); err != nil {
				b.Fatal(err)
			}
		}
		ch025PolicyCompactionRunSink, err = scheduler.Run(context.Background())
		if err != nil {
			b.Fatal(err)
		}
	}
}

func ch025TaskName(task int) string {
	return "task-" + strconv.Itoa(task)
}
