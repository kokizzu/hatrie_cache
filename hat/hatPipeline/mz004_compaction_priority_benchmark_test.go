package hatPipeline

import (
	"context"
	"sync/atomic"
	"testing"
)

var mz004PriorityBenchmarkSink uint64

func BenchmarkMZ004PrioritySchedulerSubmit(b *testing.B) {
	scheduler, err := NewPriorityScheduler(context.Background(), 1, 256)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := scheduler.SubmitPriority(context.Background(), 0, func(context.Context) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := scheduler.Wait(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkMZ004FIFOSchedulerSubmit(b *testing.B) {
	scheduler, err := NewScheduler(context.Background(), 1, 256)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := scheduler.Submit(context.Background(), func(context.Context) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := scheduler.Wait(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkMZ004PriorityFrontierSubmit(b *testing.B) {
	benchmarkMZ004FrontierSubmit(b, true)
}

func BenchmarkMZ004FIFOBoundedFrontierSubmit(b *testing.B) {
	benchmarkMZ004FrontierSubmit(b, false)
}

func benchmarkMZ004FrontierSubmit(b *testing.B, priority bool) {
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
	var submit func(context.Context, int, Task) error
	var closeScheduler func()
	var waitScheduler func() error
	if priority {
		scheduler, err := NewPriorityFrontierCompactionScheduler(context.Background(), retention, 1, 256)
		if err != nil {
			_ = retention.Close()
			_ = frontiers.Close()
			b.Fatal(err)
		}
		submit = func(ctx context.Context, priority int, task Task) error {
			return scheduler.SubmitPriority(ctx, "events", 0, priority, task)
		}
		closeScheduler = scheduler.Close
		waitScheduler = scheduler.Wait
	} else {
		scheduler, err := NewFrontierCompactionScheduler(context.Background(), retention, 1, 256)
		if err != nil {
			_ = retention.Close()
			_ = frontiers.Close()
			b.Fatal(err)
		}
		submit = func(ctx context.Context, _ int, task Task) error {
			return scheduler.Submit(ctx, "events", 0, task)
		}
		closeScheduler = scheduler.Close
		waitScheduler = scheduler.Wait
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := submit(context.Background(), 0, func(context.Context) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	closeScheduler()
	if err := waitScheduler(); err != nil {
		b.Fatal(err)
	}
	if err := retention.Close(); err != nil {
		b.Fatal(err)
	}
	if err := frontiers.Close(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkMZ004PrioritySchedulerBacklogDispatch(b *testing.B) {
	benchmarkMZ004BacklogDispatch(b, true)
}

func BenchmarkMZ004FIFOSchedulerBacklogDispatch(b *testing.B) {
	benchmarkMZ004BacklogDispatch(b, false)
}

func benchmarkMZ004BacklogDispatch(b *testing.B, priority bool) {
	b.Helper()
	const backlog = 128
	b.ReportAllocs()
	b.StopTimer()
	for range b.N {
		var submit func(context.Context, int, Task) error
		var closeScheduler func()
		var waitScheduler func() error
		if priority {
			scheduler, err := NewPriorityScheduler(context.Background(), 1, backlog+2)
			if err != nil {
				b.Fatal(err)
			}
			submit = scheduler.SubmitPriority
			closeScheduler = scheduler.Close
			waitScheduler = scheduler.Wait
		} else {
			scheduler, err := NewScheduler(context.Background(), 1, backlog+2)
			if err != nil {
				b.Fatal(err)
			}
			submit = func(ctx context.Context, _ int, task Task) error { return scheduler.Submit(ctx, task) }
			closeScheduler = scheduler.Close
			waitScheduler = scheduler.Wait
		}

		started := make(chan struct{})
		release := make(chan struct{})
		firstDone := make(chan error, 1)
		go func() {
			firstDone <- submit(context.Background(), 0, func(ctx context.Context) error {
				close(started)
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
		}()
		<-started
		for range backlog {
			if err := submit(context.Background(), 0, func(context.Context) error {
				var value uint64 = 1
				for index := 0; index < 1024; index++ {
					value = value*33 + uint64(index)
				}
				atomic.AddUint64(&mz004PriorityBenchmarkSink, value)
				return nil
			}); err != nil {
				b.Fatal(err)
			}
		}
		highDone := make(chan struct{})
		if err := submit(context.Background(), 100, func(context.Context) error {
			close(highDone)
			return nil
		}); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		close(release)
		<-highDone
		b.StopTimer()
		if err := <-firstDone; err != nil {
			b.Fatal(err)
		}
		closeScheduler()
		if err := waitScheduler(); err != nil {
			b.Fatal(err)
		}
	}
}
