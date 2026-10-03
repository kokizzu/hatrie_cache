package hatFiber

import (
	"context"
	"runtime"
	"sync"
	"testing"
)

func BenchmarkContinuationFibers(b *testing.B) {
	const (
		logicalFibers = 256
		steps         = 8
	)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		scheduler, err := NewScheduler(context.Background(), Options{
			Workers:       4,
			MaxFibers:     logicalFibers,
			QueueCapacity: logicalFibers,
		})
		if err != nil {
			b.Fatal(err)
		}
		for range logicalFibers {
			remaining := steps
			var step Step
			step = func(Context) (Step, error) {
				remaining--
				if remaining == 0 {
					return nil, nil
				}
				return step, nil
			}
			if _, err := scheduler.Spawn(context.Background(), step); err != nil {
				b.Fatal(err)
			}
		}
		if err := scheduler.Wait(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkContinuationFibersSteadyState(b *testing.B) {
	const (
		logicalFibers = 256
		steps         = 8
	)
	scheduler, err := NewScheduler(context.Background(), Options{
		Workers:       4,
		MaxFibers:     logicalFibers,
		QueueCapacity: logicalFibers,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer scheduler.Wait()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for range logicalFibers {
			remaining := steps
			var step Step
			step = func(Context) (Step, error) {
				remaining--
				if remaining == 0 {
					return nil, nil
				}
				return step, nil
			}
			if _, err := scheduler.Spawn(context.Background(), step); err != nil {
				b.Fatal(err)
			}
		}
		if err := scheduler.WaitIdle(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGoroutinePerFlow(b *testing.B) {
	const (
		logicalFibers = 256
		steps         = 8
	)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var wait sync.WaitGroup
		wait.Add(logicalFibers)
		for range logicalFibers {
			remaining := steps
			go func() {
				defer wait.Done()
				for remaining > 0 {
					remaining--
					runtime.Gosched()
				}
			}()
		}
		wait.Wait()
	}
}
