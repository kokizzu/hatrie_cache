package hatPipeline

import (
	"context"
	"sync/atomic"
	"testing"
)

var mz004CoalescingBenchmarkSink uint64

func mz004CoalescingBenchmarkWork(context.Context) error {
	var value uint64 = 1
	for index := 0; index < 512; index++ {
		value = value*33 + uint64(index)
	}
	atomic.AddUint64(&mz004CoalescingBenchmarkSink, value)
	return nil
}

func BenchmarkMZ004RedundantCompactionBaseline(b *testing.B) {
	benchmarkMZ004RedundantCompaction(b, false)
}

func BenchmarkMZ004CoalescedCompaction(b *testing.B) {
	benchmarkMZ004RedundantCompaction(b, true)
}

func benchmarkMZ004RedundantCompaction(b *testing.B, coalescing bool) {
	b.Helper()
	const duplicates = 1024
	frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if err := frontiers.Register("events"); err != nil {
		_ = frontiers.Close()
		b.Fatal(err)
	}
	if err := frontiers.Advance("events", uint64(b.N)+2, uint64(b.N)+2); err != nil {
		_ = frontiers.Close()
		b.Fatal(err)
	}
	retention, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{})
	if err != nil {
		_ = frontiers.Close()
		b.Fatal(err)
	}
	var submit func(context.Context, string, uint64, Task) (bool, error)
	var closeScheduler func()
	var waitScheduler func() error
	if coalescing {
		scheduler, err := NewCoalescingFrontierCompactionScheduler(context.Background(), retention, 1, duplicates+2)
		if err != nil {
			_ = retention.Close()
			_ = frontiers.Close()
			b.Fatal(err)
		}
		submit = scheduler.SubmitCoalesced
		closeScheduler = scheduler.Close
		waitScheduler = scheduler.Wait
	} else {
		scheduler, err := NewFrontierCompactionScheduler(context.Background(), retention, 1, duplicates+2)
		if err != nil {
			_ = retention.Close()
			_ = frontiers.Close()
			b.Fatal(err)
		}
		submit = func(ctx context.Context, frontierID string, boundary uint64, task Task) (bool, error) {
			return true, scheduler.Submit(ctx, frontierID, boundary, task)
		}
		closeScheduler = scheduler.Close
		waitScheduler = scheduler.Wait
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		boundary := uint64(index + 1)
		started := make(chan struct{})
		release := make(chan struct{})
		blockerSubmitted := make(chan error, 1)
		go func() {
			_, err := submit(context.Background(), "events", 0, func(ctx context.Context) error {
				close(started)
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			blockerSubmitted <- err
		}()
		<-started
		if err := <-blockerSubmitted; err != nil {
			b.Fatal(err)
		}
		for range duplicates {
			if _, err := submit(context.Background(), "events", boundary, mz004CoalescingBenchmarkWork); err != nil {
				b.Fatal(err)
			}
		}
		marker := make(chan struct{})
		if _, err := submit(context.Background(), "events", uint64(b.N)+1, func(context.Context) error {
			close(marker)
			return nil
		}); err != nil {
			b.Fatal(err)
		}
		close(release)
		<-marker
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
