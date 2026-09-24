package hatFiber

import (
	"context"
	"testing"
)

func benchmarkTT033SchedulerBatch(b *testing.B, options Options) {
	b.Helper()
	const fiberCount = 64
	scheduler, err := New(options)
	if err != nil {
		b.Fatal(err)
	}
	ids := make([]FiberID, fiberCount)
	spawn := func() {
		for index := range ids {
			step := func(context.Context) (Step, error) { return StepDone, nil }
			ids[index], err = scheduler.Spawn(step)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	spawn()

	ctx := context.Background()
	b.ResetTimer()
	for range b.N {
		stats, err := scheduler.Run(ctx, 0)
		if err != nil || stats.Completed != fiberCount {
			b.Fatalf("Run() = %#v/%v, want %d completed", stats, err, fiberCount)
		}
		for _, identifier := range ids {
			if err := scheduler.Reap(identifier); err != nil {
				b.Fatal(err)
			}
		}
		spawn()
	}
}

func BenchmarkTT033SchedulerBaseline(b *testing.B) {
	benchmarkTT033SchedulerBatch(b, Options{MaxFibers: 64})
}
