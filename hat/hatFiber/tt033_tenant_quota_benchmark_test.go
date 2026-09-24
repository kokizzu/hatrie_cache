package hatFiber

import (
	"context"
	"testing"
)

func tt033StepDone(context.Context) (Step, error) {
	return StepDone, nil
}

func BenchmarkTT033TenantQuota(b *testing.B) {
	for i := 0; i < b.N; i++ {
		scheduler, err := New(Options{
			MaxFibers: 64,
			TenantQuotas: map[string]TenantQuota{
				"api": {MaxFibers: 64},
			},
		})
		if err != nil {
			b.Fatal(err)
		}
		ids := make([]FiberID, 64)
		for j := range ids {
			ids[j], err = scheduler.SpawnForTenant("api", tt033StepDone)
			if err != nil {
				b.Fatal(err)
			}
		}
		stats, err := scheduler.Run(context.Background(), 0)
		if err != nil {
			b.Fatal(err)
		}
		if stats.Completed != uint64(len(ids)) {
			b.Fatalf("completed=%d want %d", stats.Completed, len(ids))
		}
		for _, id := range ids {
			if err := scheduler.Reap(id); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkTT033TenantStepQuota(b *testing.B) {
	for i := 0; i < b.N; i++ {
		scheduler, err := New(Options{
			MaxFibers: 64,
			TenantQuotas: map[string]TenantQuota{
				"api": {MaxFibers: 64, MaxStepsPerRun: 64},
			},
		})
		if err != nil {
			b.Fatal(err)
		}
		ids := make([]FiberID, 64)
		for j := range ids {
			ids[j], err = scheduler.SpawnForTenant("api", tt033StepDone)
			if err != nil {
				b.Fatal(err)
			}
		}
		stats, err := scheduler.Run(context.Background(), 0)
		if err != nil {
			b.Fatal(err)
		}
		if stats.Completed != uint64(len(ids)) {
			b.Fatalf("completed=%d want %d", stats.Completed, len(ids))
		}
		for _, id := range ids {
			if err := scheduler.Reap(id); err != nil {
				b.Fatal(err)
			}
		}
	}
}
