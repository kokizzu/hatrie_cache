package hatStorage

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestCompactionSchedulerSizeTieredSelection(t *testing.T) {
	scheduler, err := NewCompactionScheduler(CompactionSchedulerOptions{
		MaxConcurrent:   1,
		SelectionPolicy: CompactionSelectionSizeTiered,
	})
	if err != nil {
		t.Fatal(err)
	}
	var order []string
	for _, task := range []struct {
		name  string
		bytes uint64
	}{
		{name: "large", bytes: 1024},
		{name: "small-b", bytes: 9},
		{name: "medium", bytes: 64},
		{name: "small-a", bytes: 8},
	} {
		name := task.name
		if _, err := scheduler.ScheduleWithPriorityAndIO(name, 1, task.bytes, func(context.Context) error {
			order = append(order, name)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := scheduler.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if want := []string{"small-a", "small-b", "medium", "large"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("size-tiered order = %#v, want %#v", order, want)
	}
}

func TestCompactionSchedulerTimeAwareSelectionPrefersOlderQueuedWork(t *testing.T) {
	scheduler, err := NewCompactionScheduler(CompactionSchedulerOptions{
		MaxConcurrent:   1,
		SelectionPolicy: CompactionSelectionTimeAware,
	})
	if err != nil {
		t.Fatal(err)
	}
	clock := time.Unix(100, 0)
	scheduler.now = func() time.Time {
		current := clock
		clock = clock.Add(time.Second)
		return current
	}
	var order []string
	for _, name := range []string{"older", "newer"} {
		name := name
		if _, err := scheduler.ScheduleWithPriority(name, 1, func(context.Context) error {
			order = append(order, name)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := scheduler.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if want := []string{"older", "newer"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("time-aware order = %#v, want %#v", order, want)
	}
}

func TestCompactionSchedulerSelectorCoalescesAndValidates(t *testing.T) {
	scheduler, err := NewCompactionScheduler(CompactionSchedulerOptions{
		SelectionPolicy: CompactionSelectionTimeAware,
	})
	if err != nil {
		t.Fatal(err)
	}
	if accepted, err := scheduler.ScheduleWithPriorityAndIO("task", 1, 10, func(context.Context) error { return nil }); !accepted || err != nil {
		t.Fatalf("first prioritized schedule = %t/%v, want true/nil", accepted, err)
	}
	if accepted, err := scheduler.ScheduleWithPriorityAndIO("task", 3, 20, func(context.Context) error { return nil }); accepted || err != nil {
		t.Fatalf("duplicate prioritized schedule = %t/%v, want false/nil", accepted, err)
	}
	if _, err := NewCompactionScheduler(CompactionSchedulerOptions{SelectionPolicy: CompactionSelectionPolicy(99)}); !errors.Is(err, ErrCompactionSchedulerOptionsInvalid) {
		t.Fatalf("invalid selection policy error = %v, want options error", err)
	}
}
