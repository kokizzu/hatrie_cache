package hatPipeline

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMZ003FrontierCompactionSchedulerWaitsForFrontier(t *testing.T) {
	frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer frontiers.Close()
	if err := frontiers.Register("events"); err != nil {
		t.Fatal(err)
	}
	if err := frontiers.Advance("events", 0, 100); err != nil {
		t.Fatal(err)
	}
	retention, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer retention.Close()
	scheduler, err := NewFrontierCompactionScheduler(context.Background(), retention, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		scheduler.Close()
		_ = scheduler.Wait()
	}()

	ran := make(chan struct{}, 1)
	submitDone := make(chan error, 1)
	go func() {
		submitDone <- scheduler.Submit(context.Background(), "events", 10, func(context.Context) error {
			ran <- struct{}{}
			return nil
		})
	}()
	select {
	case <-ran:
		t.Fatal("compaction ran before the frontier reached its boundary")
	case <-time.After(20 * time.Millisecond):
	}
	if err := frontiers.Advance("events", 10, 100); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ran:
	case <-time.After(time.Second):
		t.Fatal("compaction did not run after the frontier advanced")
	}
	select {
	case err := <-submitDone:
		if err != nil {
			t.Fatalf("Submit() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Submit() did not return")
	}
}

func TestMZ003FrontierCompactionSchedulerWaitsForReadLeaseRelease(t *testing.T) {
	frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer frontiers.Close()
	if err := frontiers.Register("events"); err != nil {
		t.Fatal(err)
	}
	if err := frontiers.Advance("events", 0, 20); err != nil {
		t.Fatal(err)
	}
	retention, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer retention.Close()
	lease, err := retention.Acquire("events", 5)
	if err != nil {
		t.Fatal(err)
	}
	if err := frontiers.Advance("events", 10, 20); err != nil {
		t.Fatal(err)
	}
	scheduler, err := NewFrontierCompactionScheduler(context.Background(), retention, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		scheduler.Close()
		_ = scheduler.Wait()
	}()

	ran := make(chan struct{}, 1)
	submitDone := make(chan error, 1)
	go func() {
		submitDone <- scheduler.Submit(context.Background(), "events", 10, func(context.Context) error {
			ran <- struct{}{}
			return nil
		})
	}()
	select {
	case <-ran:
		t.Fatal("compaction ran while a historical read lease blocked the boundary")
	case <-time.After(20 * time.Millisecond):
	}
	if err := retention.Release(lease); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ran:
	case <-time.After(time.Second):
		t.Fatal("compaction did not run after the read lease was released")
	}
	select {
	case err := <-submitDone:
		if err != nil {
			t.Fatalf("Submit() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Submit() did not return")
	}
}

func TestMZ003FrontierCompactionSchedulerCancellationAndValidation(t *testing.T) {
	frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer frontiers.Close()
	if err := frontiers.Register("events"); err != nil {
		t.Fatal(err)
	}
	if err := frontiers.Advance("events", 0, 100); err != nil {
		t.Fatal(err)
	}
	retention, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer retention.Close()
	scheduler, err := NewFrontierCompactionScheduler(context.Background(), retention, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		scheduler.Close()
		_ = scheduler.Wait()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := scheduler.Submit(ctx, "events", 1, func(context.Context) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("Submit(canceled) error = %v, want %v", err, context.Canceled)
	}
	if err := scheduler.Submit(context.Background(), "events", 1, nil); !errors.Is(err, ErrSchedulerInvalid) {
		t.Fatalf("Submit(nil task) error = %v, want %v", err, ErrSchedulerInvalid)
	}
	if err := scheduler.Submit(context.Background(), "missing", 1, func(context.Context) error { return nil }); !errors.Is(err, ErrFrontierNotFound) {
		t.Fatalf("Submit(missing frontier) error = %v, want %v", err, ErrFrontierNotFound)
	}
}

func TestMZ003FrontierCompactionSchedulerCancelUnblocksWait(t *testing.T) {
	frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer frontiers.Close()
	if err := frontiers.Register("events"); err != nil {
		t.Fatal(err)
	}
	if err := frontiers.Advance("events", 0, 100); err != nil {
		t.Fatal(err)
	}
	retention, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer retention.Close()
	scheduler, err := NewFrontierCompactionScheduler(context.Background(), retention, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		scheduler.Close()
		_ = scheduler.Wait()
	}()

	done := make(chan error, 1)
	go func() {
		done <- scheduler.Submit(context.Background(), "events", 10, func(context.Context) error { return nil })
	}()
	time.Sleep(20 * time.Millisecond)
	scheduler.Cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Submit(after Cancel) error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(time.Second):
		t.Fatal("Cancel() did not unblock a frontier wait")
	}
}

func BenchmarkMZ003CompactionAdmissionBaseline(b *testing.B) {
	frontiers, retention := benchmarkMZ003Retention(b)
	defer frontiers.Close()
	defer retention.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		allowed, err := retention.CanCompactBefore("events", 100)
		if err != nil || !allowed {
			b.Fatalf("CanCompactBefore() = %v, %v; want true, nil", allowed, err)
		}
	}
}

func BenchmarkMZ003CompactionAdmissionAfter(b *testing.B) {
	frontiers, retention := benchmarkMZ003Retention(b)
	defer frontiers.Close()
	defer retention.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := retention.WaitUntilSafe(context.Background(), "events", 100); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkMZ003Retention(b *testing.B) (*FrontierRegistry, *FrontierRetentionRegistry) {
	b.Helper()
	frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if err := frontiers.Register("events"); err != nil {
		frontiers.Close()
		b.Fatal(err)
	}
	if err := frontiers.Advance("events", 100, 200); err != nil {
		frontiers.Close()
		b.Fatal(err)
	}
	retention, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{})
	if err != nil {
		frontiers.Close()
		b.Fatal(err)
	}
	return frontiers, retention
}
