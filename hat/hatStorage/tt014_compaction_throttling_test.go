package hatStorage_test

import (
	"context"
	"errors"
	"testing"

	"hatrie_cache/hat/hatStorage"
)

func TestCompactionControllerPendingBytesBackpressuresQueuedAndRunningWork(t *testing.T) {
	controller, err := hatStorage.NewCompactionController(hatStorage.CompactionControllerOptions{
		SchedulerOptions: hatStorage.CompactionSchedulerOptions{MaxConcurrent: 1},
		MaxPending:       4,
		MaxPendingBytes:  100,
	})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	job, accepted, err := controller.Submit(hatStorage.CompactionRequest{Target: "running", EstimatedBytes: 60, Run: func(context.Context) error {
		close(started)
		<-release
		return nil
	}})
	if err != nil || !accepted {
		t.Fatalf("Submit(running) = %#v, %t, %v", job, accepted, err)
	}

	done := make(chan error, 1)
	go func() {
		_, runErr := controller.Run(context.Background())
		done <- runErr
	}()
	<-started

	job, accepted, err = controller.Submit(hatStorage.CompactionRequest{Target: "queued-at-bound", EstimatedBytes: 40, Run: func(context.Context) error { return nil }})
	if err != nil || !accepted {
		t.Fatalf("Submit(queued-at-bound) = %#v, %t, %v", job, accepted, err)
	}
	job, accepted, err = controller.Submit(hatStorage.CompactionRequest{Target: "over-budget", EstimatedBytes: 1, Run: func(context.Context) error { return nil }})
	if accepted || !errors.Is(err, hatStorage.ErrCompactionControllerBackpressure) {
		t.Fatalf("Submit(over-budget) = %#v, %t, %v; want backpressure", job, accepted, err)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	job, accepted, err = controller.Submit(hatStorage.CompactionRequest{Target: "after-recovery", EstimatedBytes: 100, Run: func(context.Context) error { return nil }})
	if err != nil || !accepted || job.EstimatedBytes != 100 {
		t.Fatalf("Submit(after-recovery) = %#v, %t, %v", job, accepted, err)
	}
}

func TestCompactionControllerPendingBytesDefaultOff(t *testing.T) {
	controller, err := hatStorage.NewCompactionController(hatStorage.CompactionControllerOptions{MaxPending: 2})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 2; index++ {
		job, accepted, err := controller.Submit(hatStorage.CompactionRequest{
			Target:         "default-" + string(rune('a'+index)),
			EstimatedBytes: 1 << 62,
			Run:            func(context.Context) error { return nil },
		})
		if err != nil || !accepted {
			t.Fatalf("default-off Submit(%d) = %#v, %t, %v", index, job, accepted, err)
		}
	}
}
