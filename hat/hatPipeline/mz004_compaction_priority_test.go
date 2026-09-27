package hatPipeline

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMZ004PriorityFrontierCompactionRunsHigherPriorityFirst(t *testing.T) {
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
	scheduler, err := NewPriorityFrontierCompactionScheduler(context.Background(), retention, 1, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		scheduler.Close()
		_ = scheduler.Wait()
	}()

	started := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- scheduler.SubmitPriority(context.Background(), "events", 1, 0, func(ctx context.Context) error {
			close(started)
			select {
			case <-release:
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

	order := make(chan string, 2)
	if err := scheduler.SubmitPriority(context.Background(), "events", 1, 1, func(context.Context) error {
		order <- "low"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.SubmitPriority(context.Background(), "events", 1, 10, func(context.Context) error {
		order <- "high"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	close(release)

	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first submission error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first submission did not complete")
	}
	if err := scheduler.Wait(); err != nil {
		t.Fatal(err)
	}
	got := []string{<-order, <-order}
	if got[0] != "high" || got[1] != "low" {
		t.Fatalf("execution order = %v, want [high low]", got)
	}
}

func TestMZ004PrioritySubmitRequiresOptInScheduler(t *testing.T) {
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
	err = scheduler.SubmitPriority(context.Background(), "events", 0, 10, func(context.Context) error { return nil })
	if !errors.Is(err, ErrSchedulerPriorityUnavailable) {
		t.Fatalf("SubmitPriority error = %v, want %v", err, ErrSchedulerPriorityUnavailable)
	}
}

func TestMZ004PrioritySchedulerKeepsEqualPriorityFIFO(t *testing.T) {
	scheduler, err := NewPriorityScheduler(context.Background(), 1, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		scheduler.Close()
		_ = scheduler.Wait()
	}()

	started := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- scheduler.SubmitPriority(context.Background(), 0, func(ctx context.Context) error {
			close(started)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first task did not start")
	}
	results := make(chan string, 3)
	for _, name := range []string{"a", "b", "c"} {
		name := name
		if err := scheduler.SubmitPriority(context.Background(), 10, func(context.Context) error {
			results <- name
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Wait(); err != nil {
		t.Fatal(err)
	}
	got := []string{<-results, <-results, <-results}
	want := []string{"a", "b", "c"}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("equal-priority order = %v, want %v", got, want)
		}
	}
}

func TestMZ004PrioritySchedulerBoundedSubmitHonorsCancellation(t *testing.T) {
	scheduler, err := NewPriorityScheduler(context.Background(), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		scheduler.Close()
		_ = scheduler.Wait()
	}()

	started := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- scheduler.SubmitPriority(context.Background(), 0, func(ctx context.Context) error {
			close(started)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first task did not start")
	}
	if err := scheduler.SubmitPriority(context.Background(), 0, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	thirdDone := make(chan error, 1)
	go func() {
		thirdDone <- scheduler.SubmitPriority(ctx, 0, func(context.Context) error { return nil })
	}()
	select {
	case err := <-thirdDone:
		t.Fatalf("bounded submission returned before cancellation: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-thirdDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("bounded submission error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(time.Second):
		t.Fatal("bounded submission did not observe cancellation")
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
}

func TestMZ004PrioritySchedulerZeroCapacityDirectHandoff(t *testing.T) {
	scheduler, err := NewPriorityScheduler(context.Background(), 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		scheduler.Close()
		_ = scheduler.Wait()
	}()

	done := make(chan struct{})
	submitDone := make(chan error, 1)
	go func() {
		submitDone <- scheduler.SubmitPriority(context.Background(), 10, func(context.Context) error {
			close(done)
			return nil
		})
	}()
	select {
	case err := <-submitDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		scheduler.mu.Lock()
		waiting, queued := scheduler.waiting, scheduler.queue.Len()
		scheduler.mu.Unlock()
		t.Fatalf("direct-handoff submission blocked: waiting=%d queued=%d", waiting, queued)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("direct-handoff task did not run")
	}
}
