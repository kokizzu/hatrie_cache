package hatPipeline

import (
	"context"
	"testing"
)

func BenchmarkMZ004SchedulerSubmitDefault(b *testing.B) {
	benchmarkMZ004SchedulerSubmit(b, false)
}

func BenchmarkMZ004SchedulerSubmitPolicy(b *testing.B) {
	benchmarkMZ004SchedulerSubmit(b, true)
}

func benchmarkMZ004SchedulerSubmit(b *testing.B, withPolicy bool) {
	b.Helper()
	frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if err := frontiers.Register("events"); err != nil {
		_ = frontiers.Close()
		b.Fatal(err)
	}
	if err := frontiers.Advance("events", 100, 100); err != nil {
		_ = frontiers.Close()
		b.Fatal(err)
	}
	retention, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{})
	if err != nil {
		_ = frontiers.Close()
		b.Fatal(err)
	}
	scheduler, err := NewFrontierCompactionScheduler(context.Background(), retention, 1, 0)
	if err != nil {
		_ = retention.Close()
		_ = frontiers.Close()
		b.Fatal(err)
	}
	if withPolicy {
		if err := scheduler.SetPolicy("events", FrontierCompactionPolicy{MaxOutstanding: 1}); err != nil {
			scheduler.Close()
			_ = scheduler.Wait()
			_ = retention.Close()
			_ = frontiers.Close()
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := scheduler.Submit(context.Background(), "events", 0, func(context.Context) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := scheduler.Wait(); err != nil {
		b.Fatal(err)
	}
	if err := retention.Close(); err != nil {
		b.Fatal(err)
	}
	if err := frontiers.Close(); err != nil {
		b.Fatal(err)
	}
}
