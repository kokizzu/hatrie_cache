package hatStorage

import (
	"context"
	"testing"
)

func TestCHU28DefaultScheduleKeepsIOThrottleDisabled(t *testing.T) {
	scheduler, err := NewCompactionScheduler(CompactionSchedulerOptions{MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	if scheduler.ioState != nil {
		t.Fatal("default scheduler allocated I/O throttle state")
	}
	if accepted, err := scheduler.Schedule("plain", func(context.Context) error { return nil }); err != nil || !accepted {
		t.Fatalf("Schedule() = accepted %v, err %v", accepted, err)
	}
	if _, err := scheduler.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stats := scheduler.Stats(); stats.IOBytesPerSecond != 0 || stats.IOThrottledTaskCount != 0 || stats.IOWaitNanoseconds != 0 {
		t.Fatalf("default I/O stats = %#v", stats)
	}
}

func TestCHU28OptInScheduleCreatesIOThrottleState(t *testing.T) {
	scheduler, err := NewCompactionScheduler(CompactionSchedulerOptions{MaxConcurrent: 1, MaxIOBytesPerSecond: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if scheduler.ioState == nil {
		t.Fatal("opt-in scheduler did not create I/O throttle state")
	}
	if accepted, err := scheduler.ScheduleWithIO("paced", 0, func(context.Context) error { return nil }); err != nil || !accepted {
		t.Fatalf("ScheduleWithIO() = accepted %v, err %v", accepted, err)
	}
	if _, err := scheduler.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := scheduler.Stats().IOBytesPerSecond; got != 1<<20 {
		t.Fatalf("opt-in I/O rate = %d, want %d", got, 1<<20)
	}
}
