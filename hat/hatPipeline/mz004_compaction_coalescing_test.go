package hatPipeline

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newMZ004CoalescingTestScheduler(t *testing.T, priority bool) (*FrontierCompactionScheduler, *FrontierRetentionRegistry, *FrontierRegistry) {
	t.Helper()
	frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := frontiers.Register("events"); err != nil {
		_ = frontiers.Close()
		t.Fatal(err)
	}
	if err := frontiers.Advance("events", 100, 100); err != nil {
		_ = frontiers.Close()
		t.Fatal(err)
	}
	retention, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{})
	if err != nil {
		_ = frontiers.Close()
		t.Fatal(err)
	}
	var scheduler *FrontierCompactionScheduler
	if priority {
		scheduler, err = NewPriorityCoalescingFrontierCompactionScheduler(context.Background(), retention, 1, 8)
	} else {
		scheduler, err = NewCoalescingFrontierCompactionScheduler(context.Background(), retention, 1, 8)
	}
	if err != nil {
		_ = retention.Close()
		_ = frontiers.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		scheduler.Close()
		_ = scheduler.Wait()
		_ = retention.Close()
		_ = frontiers.Close()
	})
	return scheduler, retention, frontiers
}

func TestMZ004CoalescingDropsExactPendingDuplicate(t *testing.T) {
	scheduler, _, _ := newMZ004CoalescingTestScheduler(t, false)
	started := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		accepted, err := scheduler.SubmitCoalesced(context.Background(), "events", 1, func(ctx context.Context) error {
			close(started)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		if err == nil && !accepted {
			err = errors.New("first task was unexpectedly coalesced")
		}
		firstDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first compaction did not start")
	}
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	var ran atomic.Int32
	accepted, err := scheduler.SubmitCoalesced(context.Background(), "events", 1, func(context.Context) error {
		ran.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if accepted {
		t.Fatal("exact duplicate was accepted while the original was running")
	}
	accepted, err = scheduler.SubmitCoalesced(context.Background(), "events", 2, func(context.Context) error {
		ran.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !accepted {
		t.Fatal("different boundary was coalesced")
	}
	close(release)
	if err := scheduler.Wait(); err != nil {
		t.Fatal(err)
	}
	if got := ran.Load(); got != 1 {
		t.Fatalf("duplicate/different task count = %d, want only one additional task", got)
	}
}

func TestMZ004CoalescingRejectsLegacyScheduler(t *testing.T) {
	frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer frontiers.Close()
	if err := frontiers.Register("events"); err != nil {
		t.Fatal(err)
	}
	retention, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer retention.Close()
	scheduler, err := NewFrontierCompactionScheduler(context.Background(), retention, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		scheduler.Close()
		_ = scheduler.Wait()
	}()
	accepted, err := scheduler.SubmitCoalesced(context.Background(), "events", 0, func(context.Context) error { return nil })
	if !errors.Is(err, ErrSchedulerCoalescingUnavailable) {
		t.Fatalf("SubmitCoalesced error = %v, want %v", err, ErrSchedulerCoalescingUnavailable)
	}
	if accepted {
		t.Fatal("legacy scheduler accepted a coalesced task")
	}
}

func TestMZ004CoalescingCancellationReleasesKey(t *testing.T) {
	scheduler, _, frontiers := newMZ004CoalescingTestScheduler(t, false)
	// Move the frontier back to an unsafe boundary for this request by using a
	// fresh frontier name; the registry keeps the request blocked until it is
	// advanced.
	if err := frontiers.Register("late"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		accepted, err := scheduler.SubmitCoalesced(ctx, "late", 10, func(context.Context) error { return nil })
		if err == nil && !accepted {
			err = errors.New("blocked request was unexpectedly coalesced")
		}
		result <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled coalesced submit error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled coalesced submit did not return")
	}
	if err := frontiers.Advance("late", 10, 10); err != nil {
		t.Fatal(err)
	}
	accepted, err := scheduler.SubmitCoalesced(context.Background(), "late", 10, func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if !accepted {
		t.Fatal("canceled request kept its coalescing key")
	}
}

func TestMZ004CoalescingConcurrentDuplicateOnlyAcceptsOnce(t *testing.T) {
	scheduler, _, _ := newMZ004CoalescingTestScheduler(t, true)
	started := make(chan struct{})
	release := make(chan struct{})
	accepted, err := scheduler.SubmitPriorityCoalesced(context.Background(), "events", 1, 0, func(ctx context.Context) error {
		close(started)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if err != nil || !accepted {
		t.Fatalf("initial priority coalesced submit = %v/%v, want accepted", accepted, err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("initial priority task did not start")
	}
	const duplicateCount = 32
	var group sync.WaitGroup
	var acceptedCount atomic.Int32
	firstErr := make(chan error, 1)
	for range duplicateCount {
		group.Add(1)
		go func() {
			defer group.Done()
			accepted, err := scheduler.SubmitPriorityCoalesced(context.Background(), "events", 1, 0, func(context.Context) error { return nil })
			if err != nil {
				select {
				case firstErr <- err:
				default:
				}
				return
			}
			if accepted {
				acceptedCount.Add(1)
			}
		}()
	}
	group.Wait()
	select {
	case err := <-firstErr:
		t.Fatal(err)
	default:
	}
	if got := acceptedCount.Load(); got != 0 {
		t.Fatalf("concurrent duplicate accepted count = %d, want 0", got)
	}
	close(release)
}
