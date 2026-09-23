package hatFiber

import (
	"context"
	"testing"
)

func BenchmarkT235WorkerPool(b *testing.B) {
	const (
		workerCount = 4
		fiberCount  = 32
		taskCount   = 128
		stepCount   = 8
	)

	pool, err := NewWorkerPool(WorkerPoolOptions{
		Workers:            workerCount,
		MaxFibersPerWorker: fiberCount,
		QueueCapacity:      taskCount,
		StepsPerTurn:       stepCount,
	})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		futures := make([]*Future, taskCount)
		for index := range futures {
			remaining := stepCount
			future, err := pool.Submit(ctx, func(context.Context) (Step, error) {
				remaining--
				if remaining == 0 {
					return StepDone, nil
				}
				return StepYield, nil
			})
			if err != nil {
				b.Fatal(err)
			}
			futures[index] = future
		}
		for _, future := range futures {
			if err := future.Wait(ctx); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()
	if err := pool.Close(ctx); err != nil {
		b.Fatal(err)
	}
}
