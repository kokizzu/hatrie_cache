package hatStorage_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatStorage"
)

var chu28SchedulerRunSink hatStorage.CompactionRun

func BenchmarkCHU28SchedulerRunBaseline(b *testing.B) {
	for range b.N {
		scheduler, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{MaxConcurrent: 1})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := scheduler.Schedule("ch-u28", func(context.Context) error { return nil }); err != nil {
			b.Fatal(err)
		}
		chu28SchedulerRunSink, err = scheduler.Run(context.Background())
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCHU28SchedulerRunThrottled(b *testing.B) {
	for range b.N {
		scheduler, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{
			MaxConcurrent:       1,
			MaxIOBytesPerSecond: 1_000_000_000_000,
		})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := scheduler.ScheduleWithIO("ch-u28", 1, func(context.Context) error { return nil }); err != nil {
			b.Fatal(err)
		}
		chu28SchedulerRunSink, err = scheduler.Run(context.Background())
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCHU28SchedulerRunBaselineWarm(b *testing.B) {
	scheduler, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{MaxConcurrent: 1})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := scheduler.Schedule("ch-u28", func(context.Context) error { return nil }); err != nil {
			b.Fatal(err)
		}
		chu28SchedulerRunSink, err = scheduler.Run(context.Background())
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCHU28SchedulerRunThrottledWarm(b *testing.B) {
	scheduler, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{
		MaxConcurrent:       1,
		MaxIOBytesPerSecond: 1_000_000_000_000,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := scheduler.ScheduleWithIO("ch-u28", 1, func(context.Context) error { return nil }); err != nil {
			b.Fatal(err)
		}
		chu28SchedulerRunSink, err = scheduler.Run(context.Background())
		if err != nil {
			b.Fatal(err)
		}
	}
}
