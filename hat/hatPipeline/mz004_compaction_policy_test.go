package hatPipeline

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMZ004FrontierCompactionSchedulerLimitsOutstandingPerCollection(t *testing.T) {
	frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer frontiers.Close()
	if err := frontiers.Register("events"); err != nil {
		t.Fatal(err)
	}
	if err := frontiers.Advance("events", 100, 100); err != nil {
		t.Fatal(err)
	}
	retention, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer retention.Close()
	scheduler, err := NewFrontierCompactionScheduler(context.Background(), retention, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		scheduler.Close()
		_ = scheduler.Wait()
	}()
	if err := scheduler.SetPolicy("", FrontierCompactionPolicy{MaxOutstanding: 1}); !errors.Is(err, ErrFrontierCompactionPolicyInvalid) {
		t.Fatalf("SetPolicy(empty frontier) error = %v, want %v", err, ErrFrontierCompactionPolicyInvalid)
	}
	if err := scheduler.SetPolicy("events", FrontierCompactionPolicy{}); !errors.Is(err, ErrFrontierCompactionPolicyInvalid) {
		t.Fatalf("SetPolicy(zero limit) error = %v, want %v", err, ErrFrontierCompactionPolicyInvalid)
	}
	if err := scheduler.SetPolicy("events", FrontierCompactionPolicy{MaxOutstanding: 1}); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{}, 2)
	releaseFirst := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- scheduler.Submit(context.Background(), "events", 1, func(ctx context.Context) error {
			started <- struct{}{}
			select {
			case <-releaseFirst:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first compaction did not start")
	}

	secondDone := make(chan error, 1)
	go func() {
		secondDone <- scheduler.Submit(context.Background(), "events", 2, func(context.Context) error {
			started <- struct{}{}
			return nil
		})
	}()
	select {
	case err := <-secondDone:
		t.Fatalf("second submission returned before first completed: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	select {
	case <-started:
		t.Fatal("second compaction started before the first completed")
	default:
	}

	close(releaseFirst)
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first submission error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first submission did not complete")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("second compaction did not start after the first completed")
	}
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("second submission error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second submission did not complete")
	}
}

func TestMZ004FrontierCompactionSchedulerPolicyWaitHonorsCancellation(t *testing.T) {
	frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer frontiers.Close()
	if err := frontiers.Register("events"); err != nil {
		t.Fatal(err)
	}
	if err := frontiers.Advance("events", 100, 100); err != nil {
		t.Fatal(err)
	}
	retention, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer retention.Close()
	scheduler, err := NewFrontierCompactionScheduler(context.Background(), retention, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		scheduler.Close()
		_ = scheduler.Wait()
	}()
	if err := scheduler.SetPolicy("events", FrontierCompactionPolicy{MaxOutstanding: 1}); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{}, 1)
	releaseFirst := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- scheduler.Submit(context.Background(), "events", 1, func(ctx context.Context) error {
			started <- struct{}{}
			select {
			case <-releaseFirst:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first compaction did not start")
	}

	ctx, cancel := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- scheduler.Submit(ctx, "events", 2, func(context.Context) error { return nil })
	}()
	select {
	case err := <-secondDone:
		t.Fatalf("capped submission returned before cancellation: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-secondDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("capped submission error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("capped submission did not observe cancellation")
	}
	close(releaseFirst)
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first submission error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first submission did not complete")
	}
}
