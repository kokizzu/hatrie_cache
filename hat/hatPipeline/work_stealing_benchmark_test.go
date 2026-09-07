package hatPipeline_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatPipeline"
)

func BenchmarkWorkStealingPoolBatch(b *testing.B) {
	benchmarkTaskPool(b, func() (taskPool, error) {
		return hatPipeline.NewWorkStealingPool(context.Background(), 4, 64)
	})
}

func BenchmarkSchedulerBatch(b *testing.B) {
	benchmarkTaskPool(b, func() (taskPool, error) {
		return hatPipeline.NewScheduler(context.Background(), 4, 64)
	})
}

type taskPool interface {
	Submit(context.Context, hatPipeline.Task) error
	Close()
	Wait() error
}

func benchmarkTaskPool(b *testing.B, newPool func() (taskPool, error)) {
	for _, tasks := range []int{8, 64, 256} {
		b.Run(testTaskCountName(tasks), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				pool, err := newPool()
				if err != nil {
					b.Fatal(err)
				}
				for range tasks {
					if err := pool.Submit(context.Background(), func(context.Context) error { return nil }); err != nil {
						b.Fatal(err)
					}
				}
				pool.Close()
				if err := pool.Wait(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func testTaskCountName(tasks int) string {
	switch tasks {
	case 8:
		return "tasks_8"
	case 64:
		return "tasks_64"
	default:
		return "tasks_256"
	}
}
