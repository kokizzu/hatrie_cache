package hatCache

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestTR013CompactionDebtSchedulerRunsAtThresholdAndRetainsFailureDebt(t *testing.T) {
	var calls atomic.Int32
	shouldFail := true
	scheduler, err := NewCompactionDebtScheduler(CompactionDebtSchedulerOptions{
		ThresholdBytes: 100,
		CheckInterval:  time.Millisecond,
		Compact: func(LevelDBCompactionOptions) (LevelDBCompactionResult, error) {
			calls.Add(1)
			if shouldFail {
				return LevelDBCompactionResult{}, errors.New("compaction unavailable")
			}
			return LevelDBCompactionResult{Store: "test"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.AddDebt(99); err != nil {
		t.Fatal(err)
	}
	if _, due, err := scheduler.CompactIfDue(); err != nil || due {
		t.Fatalf("CompactIfDue below threshold = due:%v err:%v, want false/nil", due, err)
	}
	if got := scheduler.DebtBytes(); got != 99 {
		t.Fatalf("debt below threshold = %d, want 99", got)
	}
	if err := scheduler.AddDebt(1); err != nil {
		t.Fatal(err)
	}
	if _, due, err := scheduler.CompactIfDue(); !due || !errors.Is(err, ErrCompactionDebtSchedulerCompaction) {
		t.Fatalf("failed CompactIfDue = due:%v err:%v, want due and wrapped error", due, err)
	}
	if got := scheduler.DebtBytes(); got != 100 {
		t.Fatalf("debt after failed compaction = %d, want retained 100", got)
	}
	shouldFail = false
	result, due, err := scheduler.CompactIfDue()
	if err != nil || !due || result.Store != "test" {
		t.Fatalf("successful CompactIfDue = %#v due:%v err:%v", result, due, err)
	}
	if got := scheduler.DebtBytes(); got != 0 {
		t.Fatalf("debt after successful compaction = %d, want 0", got)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("compaction calls = %d, want 2", got)
	}
}

func TestTR013CompactionDebtSchedulerPreservesDebtAddedDuringCompaction(t *testing.T) {
	started := make(chan struct{})
	continueCompaction := make(chan struct{})
	scheduler, err := NewCompactionDebtScheduler(CompactionDebtSchedulerOptions{
		ThresholdBytes: 10,
		Compact: func(LevelDBCompactionOptions) (LevelDBCompactionResult, error) {
			close(started)
			<-continueCompaction
			return LevelDBCompactionResult{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.AddDebt(10); err != nil {
		t.Fatal(err)
	}
	resultCh := make(chan error, 1)
	go func() {
		_, _, runErr := scheduler.CompactIfDue()
		resultCh <- runErr
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("compaction did not start")
	}
	if err := scheduler.AddDebt(7); err != nil {
		t.Fatal(err)
	}
	close(continueCompaction)
	if err := <-resultCh; err != nil {
		t.Fatal(err)
	}
	if got := scheduler.DebtBytes(); got != 7 {
		t.Fatalf("debt added during compaction = %d, want 7", got)
	}
}

func TestTR013CompactionDebtSchedulerRunStopsOnContext(t *testing.T) {
	scheduler, err := NewCompactionDebtScheduler(CompactionDebtSchedulerOptions{
		ThresholdBytes: 1,
		CheckInterval:  time.Millisecond,
		Compact: func(LevelDBCompactionOptions) (LevelDBCompactionResult, error) {
			return LevelDBCompactionResult{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan error, 1)
	go func() { resultCh <- scheduler.Run(ctx) }()
	cancel()
	select {
	case err := <-resultCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("scheduler did not stop after context cancellation")
	}
}
