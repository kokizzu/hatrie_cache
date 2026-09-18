//go:build !chu27baseline

package hatStorage_test

import (
	"context"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatStorage"
)

func TestCHU27CompactionSchedulerRunsHigherPriorityTasksFirst(t *testing.T) {
	scheduler, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{MaxConcurrent: 1})
	if err != nil {
		t.Fatalf("NewCompactionScheduler() error = %v", err)
	}
	var order []string
	record := func(name string) func(context.Context) error {
		return func(context.Context) error {
			order = append(order, name)
			return nil
		}
	}
	if _, err := scheduler.ScheduleWithPriority("low", 1, record("low")); err != nil {
		t.Fatalf("ScheduleWithPriority(low) error = %v", err)
	}
	if _, err := scheduler.ScheduleWithPriority("urgent", 100, record("urgent")); err != nil {
		t.Fatalf("ScheduleWithPriority(urgent) error = %v", err)
	}
	if _, err := scheduler.Schedule("normal", record("normal")); err != nil {
		t.Fatalf("Schedule(normal) error = %v", err)
	}
	if _, err := scheduler.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if want := []string{"urgent", "low", "normal"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("execution order = %v, want %v", order, want)
	}
}

func TestCHU27CompactionSchedulerRaisesPriorityForPendingDuplicate(t *testing.T) {
	scheduler, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{MaxConcurrent: 1})
	if err != nil {
		t.Fatalf("NewCompactionScheduler() error = %v", err)
	}
	var order []string
	if _, err := scheduler.ScheduleWithPriority("background", 1, func(context.Context) error {
		order = append(order, "background")
		return nil
	}); err != nil {
		t.Fatalf("initial ScheduleWithPriority() error = %v", err)
	}
	if _, err := scheduler.ScheduleWithPriority("urgent", 2, func(context.Context) error {
		order = append(order, "urgent")
		return nil
	}); err != nil {
		t.Fatalf("urgent ScheduleWithPriority() error = %v", err)
	}
	queued, err := scheduler.ScheduleWithPriority("background", 100, func(context.Context) error {
		order = append(order, "replacement")
		return nil
	})
	if err != nil {
		t.Fatalf("duplicate ScheduleWithPriority() error = %v", err)
	}
	if queued {
		t.Fatal("duplicate ScheduleWithPriority() reported a new queue entry")
	}
	if _, err := scheduler.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if want := []string{"background", "urgent"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("execution order = %v, want %v", order, want)
	}
}

func TestCHU27CompactionSchedulerRetryRetainsPriority(t *testing.T) {
	scheduler, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{MaxConcurrent: 1})
	if err != nil {
		t.Fatalf("NewCompactionScheduler() error = %v", err)
	}
	attempts := 0
	if _, err := scheduler.ScheduleWithPriority("low", 1, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("ScheduleWithPriority(low) error = %v", err)
	}
	if _, err := scheduler.ScheduleWithPriority("urgent", 100, func(context.Context) error {
		attempts++
		if attempts == 1 {
			return context.Canceled
		}
		return nil
	}); err != nil {
		t.Fatalf("ScheduleWithPriority(urgent) error = %v", err)
	}
	if _, err := scheduler.Run(context.Background()); err == nil {
		t.Fatal("first Run() error = nil, want retry error")
	}
	if scheduler.Pending() != 1 {
		t.Fatalf("Pending() after failed priority task = %d, want 1", scheduler.Pending())
	}
	if _, err := scheduler.Run(context.Background()); err != nil {
		t.Fatalf("retry Run() error = %v", err)
	}
	if attempts != 2 || scheduler.Pending() != 0 {
		t.Fatalf("retry state = attempts:%d pending:%d, want attempts:2 pending:0", attempts, scheduler.Pending())
	}
}
