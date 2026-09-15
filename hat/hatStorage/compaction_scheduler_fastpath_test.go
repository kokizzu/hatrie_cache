package hatStorage

import (
	"context"
	"errors"
	"testing"
)

func TestCompactionSchedulerSingleTaskCanQueueFollowup(t *testing.T) {
	scheduler, err := NewCompactionScheduler(CompactionSchedulerOptions{MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	queued := false
	if _, err := scheduler.Schedule("first", func(context.Context) error {
		var scheduleErr error
		queued, scheduleErr = scheduler.Schedule("followup", func(context.Context) error { return nil })
		return scheduleErr
	}); err != nil {
		t.Fatal(err)
	}
	result, err := scheduler.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !queued || result.Scheduled != 1 || result.Completed != 1 || result.Failed != 0 {
		t.Fatalf("first run = %#v, queued=%v", result, queued)
	}
	result, err = scheduler.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Scheduled != 1 || result.Completed != 1 || result.Failed != 0 {
		t.Fatalf("follow-up run = %#v", result)
	}
}

func TestCompactionSchedulerSingleTaskFailureIsRetried(t *testing.T) {
	scheduler, err := NewCompactionScheduler(CompactionSchedulerOptions{MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("retry")
	attempts := 0
	if _, err := scheduler.Schedule("retry", func(context.Context) error {
		attempts++
		if attempts == 1 {
			return wantErr
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	result, err := scheduler.Run(context.Background())
	if !errors.Is(err, wantErr) {
		t.Fatalf("first run error = %v, want %v", err, wantErr)
	}
	if result.Scheduled != 1 || result.Completed != 0 || result.Failed != 1 || scheduler.Pending() != 1 {
		t.Fatalf("first run = %#v, pending=%d", result, scheduler.Pending())
	}
	result, err = scheduler.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Scheduled != 1 || result.Completed != 1 || result.Failed != 0 || scheduler.Pending() != 0 || attempts != 2 {
		t.Fatalf("retry run = %#v, pending=%d, attempts=%d", result, scheduler.Pending(), attempts)
	}
}
