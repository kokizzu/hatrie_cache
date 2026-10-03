package hatPipeline

import (
	"context"
	"testing"
)

func BenchmarkTT033SchedulerLifecycle(b *testing.B) {
	b.Run("Scheduler", func(b *testing.B) {
		for range b.N {
			scheduler, err := NewScheduler(context.Background(), 1, 1)
			if err != nil {
				b.Fatal(err)
			}
			if err := scheduler.Submit(context.Background(), func(context.Context) error { return nil }); err != nil {
				b.Fatal(err)
			}
			if err := scheduler.Wait(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("TenantQuotaScheduler", func(b *testing.B) {
		for range b.N {
			scheduler, err := NewTenantQuotaScheduler(TenantQuotaSchedulerOptions{
				Workers:       1,
				QueueCapacity: 1,
				DefaultPolicy: TenantQuotaPolicy{MaxConcurrent: 1, MaxQueued: 0},
			})
			if err != nil {
				b.Fatal(err)
			}
			if err := scheduler.Submit(context.Background(), "tenant-a", func(context.Context) error { return nil }); err != nil {
				b.Fatal(err)
			}
			if err := scheduler.Wait(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
