package hatPipeline

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestTT033TenantQuotaSchedulerEnforcesPerTenantQueue(t *testing.T) {
	scheduler, err := NewTenantQuotaScheduler(TenantQuotaSchedulerOptions{
		Workers:       2,
		QueueCapacity: 4,
		DefaultPolicy: TenantQuotaPolicy{MaxConcurrent: 1, MaxQueued: 1},
	})
	if err != nil {
		t.Fatal(err)
	}

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	if err := scheduler.Submit(context.Background(), "tenant-a", func(context.Context) error {
		close(firstStarted)
		<-releaseFirst
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	<-firstStarted

	secondResult := make(chan error, 1)
	secondStarted := make(chan struct{})
	go func() {
		secondResult <- scheduler.Submit(context.Background(), "tenant-a", func(context.Context) error {
			close(secondStarted)
			return nil
		})
	}()
	waitForTT033TenantStats(t, scheduler, "tenant-a", func(stats TenantQuotaTenantStats) bool {
		return stats.WaitingTasks == 1
	})

	if err := scheduler.Submit(context.Background(), "tenant-a", func(context.Context) error { return nil }); !errors.Is(err, ErrTenantQuotaQueueFull) {
		t.Fatalf("third same-tenant submit error = %v, want ErrTenantQuotaQueueFull", err)
	}

	close(releaseFirst)
	if err := <-secondResult; err != nil {
		t.Fatal(err)
	}
	<-secondStarted
	if err := scheduler.Wait(); err != nil {
		t.Fatal(err)
	}
	if _, ok := scheduler.TenantStats("tenant-a"); ok {
		t.Fatal("tenant state remained after all work completed")
	}
}

func TestTT033TenantQuotaSchedulerDoesNotThrottleIndependentTenants(t *testing.T) {
	scheduler, err := NewTenantQuotaScheduler(TenantQuotaSchedulerOptions{
		Workers:       2,
		QueueCapacity: 2,
		DefaultPolicy: TenantQuotaPolicy{MaxConcurrent: 1, MaxQueued: 0},
	})
	if err != nil {
		t.Fatal(err)
	}

	startedA := make(chan struct{})
	startedB := make(chan struct{})
	release := make(chan struct{})
	for _, testCase := range []struct {
		tenant string
		start  chan struct{}
	}{
		{tenant: "tenant-a", start: startedA},
		{tenant: "tenant-b", start: startedB},
	} {
		caseData := testCase
		if err := scheduler.Submit(context.Background(), caseData.tenant, func(context.Context) error {
			close(caseData.start)
			<-release
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-startedA:
	case <-time.After(time.Second):
		t.Fatal("tenant-a task did not start")
	}
	select {
	case <-startedB:
	case <-time.After(time.Second):
		t.Fatal("tenant-b task did not start independently")
	}
	close(release)
	if err := scheduler.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestTT033TenantQuotaSchedulerWaitingSubmitHonorsCancellation(t *testing.T) {
	scheduler, err := NewTenantQuotaScheduler(TenantQuotaSchedulerOptions{
		Workers:       1,
		QueueCapacity: 1,
		DefaultPolicy: TenantQuotaPolicy{MaxConcurrent: 1, MaxQueued: 1},
	})
	if err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	if err := scheduler.Submit(context.Background(), "tenant-a", func(context.Context) error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- scheduler.Submit(ctx, "tenant-a", func(context.Context) error { return nil })
	}()
	waitForTT033TenantStats(t, scheduler, "tenant-a", func(stats TenantQuotaTenantStats) bool {
		return stats.WaitingTasks == 1
	})
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiting submit error = %v, want context.Canceled", err)
	}
	close(release)
	if err := scheduler.Wait(); err != nil {
		t.Fatal(err)
	}
	if stats := scheduler.Stats(); stats.ActiveTenants != 0 || stats.RunningTasks != 0 || stats.WaitingTasks != 0 {
		t.Fatalf("stats after cancellation = %+v, want no active work", stats)
	}
}

func TestTT033TenantQuotaSchedulerWaitCleansDiscardedTasks(t *testing.T) {
	scheduler, err := NewTenantQuotaScheduler(TenantQuotaSchedulerOptions{
		Workers:       1,
		QueueCapacity: 1,
		DefaultPolicy: TenantQuotaPolicy{MaxConcurrent: 2, MaxQueued: 2},
	})
	if err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	if err := scheduler.Submit(context.Background(), "tenant-a", func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := scheduler.Submit(context.Background(), "tenant-a", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	scheduler.Cancel()
	if err := scheduler.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait() error = %v, want context.Canceled", err)
	}
	if stats := scheduler.Stats(); stats.ActiveTenants != 0 || stats.RunningTasks != 0 || stats.WaitingTasks != 0 {
		t.Fatalf("stats after discarded task cleanup = %+v, want no active work", stats)
	}
}

func TestTT033TenantQuotaSchedulerTaskErrorAndDefaults(t *testing.T) {
	scheduler, err := NewTenantQuotaScheduler(TenantQuotaSchedulerOptions{
		Workers:       1,
		QueueCapacity: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("task failed")
	if err := scheduler.Submit(context.Background(), "tenant-a", func(context.Context) error { return wantErr }); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Wait(); !errors.Is(err, wantErr) {
		t.Fatalf("Wait() error = %v, want task error", err)
	}
	if stats := scheduler.Stats(); stats.AcceptedTasks != 1 || stats.ActiveTenants != 0 {
		t.Fatalf("final stats = %+v, want one accepted task and no active tenants", stats)
	}
}

func TestTT033TenantQuotaSchedulerTracksConcurrency(t *testing.T) {
	scheduler, err := NewTenantQuotaScheduler(TenantQuotaSchedulerOptions{
		Workers:       2,
		QueueCapacity: 2,
		DefaultPolicy: TenantQuotaPolicy{MaxConcurrent: 2, MaxQueued: 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	var current atomic.Int32
	var maximum atomic.Int32
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	for range 2 {
		if err := scheduler.Submit(context.Background(), "tenant-a", func(context.Context) error {
			value := current.Add(1)
			for {
				previous := maximum.Load()
				if value <= previous || maximum.CompareAndSwap(previous, value) {
					break
				}
			}
			started <- struct{}{}
			<-release
			current.Add(-1)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	<-started
	<-started
	if got := maximum.Load(); got != 2 {
		t.Fatalf("maximum concurrent tasks = %d, want 2", got)
	}
	close(release)
	if err := scheduler.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestTT033TenantQuotaSchedulerValidatesAndCloses(t *testing.T) {
	for _, policy := range []TenantQuotaPolicy{
		{MaxConcurrent: -1},
		{MaxConcurrent: MaxTenantQuotaMaxConcurrent + 1},
		{MaxQueued: -1},
		{MaxQueued: MaxTenantQuotaMaxQueued + 1},
	} {
		if _, err := NewTenantQuotaScheduler(TenantQuotaSchedulerOptions{
			Workers:       1,
			QueueCapacity: 1,
			DefaultPolicy: policy,
		}); !errors.Is(err, ErrTenantQuotaPolicyInvalid) {
			t.Fatalf("policy %+v error = %v, want ErrTenantQuotaPolicyInvalid", policy, err)
		}
	}

	scheduler, err := NewTenantQuotaScheduler(TenantQuotaSchedulerOptions{Workers: 1, QueueCapacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Submit(context.Background(), " ", func(context.Context) error { return nil }); !errors.Is(err, ErrTenantQuotaTenantRequired) {
		t.Fatalf("empty tenant error = %v, want ErrTenantQuotaTenantRequired", err)
	}
	if err := scheduler.Submit(context.Background(), "tenant-a", nil); !errors.Is(err, ErrTenantQuotaTaskInvalid) {
		t.Fatalf("nil task error = %v, want ErrTenantQuotaTaskInvalid", err)
	}
	scheduler.Close()
	if err := scheduler.Submit(context.Background(), "tenant-a", func(context.Context) error { return nil }); !errors.Is(err, ErrTenantQuotaClosed) {
		t.Fatalf("submit after close error = %v, want ErrTenantQuotaClosed", err)
	}
	if err := scheduler.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestTT033TenantQuotaSchedulerEvaluatesPolicyWhenTenantStarts(t *testing.T) {
	var calls atomic.Int32
	scheduler, err := NewTenantQuotaScheduler(TenantQuotaSchedulerOptions{
		Workers:       2,
		QueueCapacity: 2,
		PolicyFor: func(string) TenantQuotaPolicy {
			calls.Add(1)
			return TenantQuotaPolicy{MaxConcurrent: 2, MaxQueued: 0}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	task := func(context.Context) error {
		started <- struct{}{}
		<-release
		return nil
	}
	if err := scheduler.Submit(context.Background(), "tenant-a", task); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := scheduler.Submit(context.Background(), "tenant-a", task); err != nil {
		t.Fatal(err)
	}
	<-started
	close(release)
	if err := scheduler.Wait(); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("policy callback calls = %d, want 1 while tenant is active", got)
	}
}

func waitForTT033TenantStats(t *testing.T, scheduler *TenantQuotaScheduler, tenant string, predicate func(TenantQuotaTenantStats) bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if stats, ok := scheduler.TenantStats(tenant); ok && predicate(stats) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	stats, ok := scheduler.TenantStats(tenant)
	t.Fatalf("tenant stats did not reach expected state: stats=%+v, present=%v", stats, ok)
}
