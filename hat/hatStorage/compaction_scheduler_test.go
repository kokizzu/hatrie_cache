package hatStorage_test

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"hatrie_cache/hat/hatStorage"
)

func TestCompactionSchedulerCoalescesAndBoundsConcurrentJobs(t *testing.T) {
	scheduler, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{MaxConcurrent: 2})
	if err != nil {
		t.Fatal(err)
	}
	var current, maximum atomic.Int32
	started := make(chan struct{}, 3)
	release := make(chan struct{})
	run := func(context.Context) error {
		inFlight := current.Add(1)
		for {
			previous := maximum.Load()
			if inFlight <= previous || maximum.CompareAndSwap(previous, inFlight) {
				break
			}
		}
		started <- struct{}{}
		<-release
		current.Add(-1)
		return nil
	}
	for _, name := range []string{"b", "a"} {
		accepted, err := scheduler.Schedule(name, run)
		if err != nil || !accepted {
			t.Fatalf("Schedule(%q) = %t, %v", name, accepted, err)
		}
	}
	accepted, err := scheduler.Schedule("a", run)
	if err != nil || accepted {
		t.Fatalf("duplicate Schedule(a) = %t, %v; want false, nil", accepted, err)
	}
	finished := make(chan struct {
		run hatStorage.CompactionRun
		err error
	}, 1)
	go func() {
		run, err := scheduler.Run(context.Background())
		finished <- struct {
			run hatStorage.CompactionRun
			err error
		}{run, err}
	}()
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for bounded jobs")
		}
	}
	if got := maximum.Load(); got != 2 {
		t.Fatalf("maximum concurrent jobs = %d, want 2", got)
	}
	select {
	case <-started:
		t.Fatal("third job started before a slot was released")
	default:
	}
	close(release)
	result := <-finished
	if result.err != nil {
		t.Fatal(result.err)
	}
	if result.run.Scheduled != 2 || result.run.Completed != 2 || result.run.Failed != 0 {
		t.Fatalf("run = %#v, want two completed jobs", result.run)
	}
	if got := scheduler.Pending(); got != 0 {
		t.Fatalf("pending jobs = %d, want 0", got)
	}
}

func TestCompactionSchedulerRetriesFailedJobs(t *testing.T) {
	scheduler, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	accepted, err := scheduler.Schedule("shard-a", func(context.Context) error {
		if attempts.Add(1) == 1 {
			return errors.New("temporary compaction failure")
		}
		return nil
	})
	if err != nil || !accepted {
		t.Fatalf("Schedule() = %t, %v", accepted, err)
	}
	first, err := scheduler.Run(context.Background())
	if err == nil || first.Completed != 0 || first.Failed != 1 || scheduler.Pending() != 1 {
		t.Fatalf("first run = %#v, err=%v, pending=%d", first, err, scheduler.Pending())
	}
	second, err := scheduler.Run(context.Background())
	if err != nil || second.Completed != 1 || second.Failed != 0 || scheduler.Pending() != 0 {
		t.Fatalf("second run = %#v, err=%v, pending=%d", second, err, scheduler.Pending())
	}
}

func TestCompactionSchedulerValidatesInputsAndNilReceivers(t *testing.T) {
	if _, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{MaxConcurrent: -1}); !errors.Is(err, hatStorage.ErrCompactionSchedulerOptionsInvalid) {
		t.Fatalf("negative concurrency error = %v", err)
	}
	var scheduler *hatStorage.CompactionScheduler
	if _, err := scheduler.Schedule("x", func(context.Context) error { return nil }); !errors.Is(err, hatStorage.ErrCompactionSchedulerNil) {
		t.Fatalf("nil Schedule error = %v", err)
	}
	if _, err := scheduler.Run(context.Background()); !errors.Is(err, hatStorage.ErrCompactionSchedulerNil) {
		t.Fatalf("nil Run error = %v", err)
	}
	valid, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"", "  "} {
		if _, err := valid.Schedule(name, func(context.Context) error { return nil }); !errors.Is(err, hatStorage.ErrCompactionTaskInvalid) {
			t.Fatalf("Schedule(%q) error = %v", name, err)
		}
	}
	if _, err := valid.Schedule("x", nil); !errors.Is(err, hatStorage.ErrCompactionTaskInvalid) {
		t.Fatalf("nil callback error = %v", err)
	}
}

func BenchmarkCompactionSchedulerRun(b *testing.B) {
	for range b.N {
		scheduler, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{MaxConcurrent: 4})
		if err != nil {
			b.Fatal(err)
		}
		for index := 0; index < 64; index++ {
			if _, err := scheduler.Schedule(strconv.Itoa(index), func(context.Context) error { return nil }); err != nil {
				b.Fatal(err)
			}
		}
		if result, err := scheduler.Run(context.Background()); err != nil || result.Completed != 64 {
			b.Fatalf("Run() = %#v, %v", result, err)
		}
	}
}
