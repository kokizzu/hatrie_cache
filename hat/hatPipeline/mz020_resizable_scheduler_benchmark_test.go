package hatPipeline

import (
	"context"
	"testing"
)

func BenchmarkFixedSchedulerNoopBatch(b *testing.B) {
	const tasks = 256
	noOp := func(context.Context) error { return nil }
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scheduler, err := NewScheduler(context.Background(), 4, tasks)
		if err != nil {
			b.Fatal(err)
		}
		for range tasks {
			if err := scheduler.Submit(context.Background(), noOp); err != nil {
				b.Fatal(err)
			}
		}
		if err := scheduler.Wait(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkResizableSchedulerNoopBatch(b *testing.B) {
	const tasks = 256
	noOp := func(context.Context) error { return nil }
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scheduler, err := NewResizableScheduler(context.Background(), 4, tasks)
		if err != nil {
			b.Fatal(err)
		}
		for range tasks {
			if err := scheduler.Submit(context.Background(), noOp); err != nil {
				b.Fatal(err)
			}
		}
		if err := scheduler.Wait(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkResizableSchedulerResize(b *testing.B) {
	scheduler, err := NewResizableScheduler(context.Background(), 1, 1)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := scheduler.Resize(4); err != nil {
			b.Fatal(err)
		}
		if err := scheduler.Resize(1); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := scheduler.Wait(); err != nil {
		b.Fatal(err)
	}
}
