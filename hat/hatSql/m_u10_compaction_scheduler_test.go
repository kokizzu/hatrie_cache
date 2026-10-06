package hatSql

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDifferentialTemporalJoinCompactionSchedulerRunsAndStops(t *testing.T) {
	join := newMUTenSchedulerJoin(t, &DifferentialTemporalJoinCompactionPolicy{
		MinUpdates:           1,
		MaxStateBytes:        1 << 20,
		MaxFrontierAge:       time.Hour,
		EstimatedBytesPerRow: 64,
	})
	if _, err := join.ApplyLeft([]DifferentialRow{{Key: "left", Time: 1, Diff: 1, Row: Row{"group": "g"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := join.ApplyRight([]DifferentialRow{{Key: "right", Time: 1, Diff: 1, Row: Row{"group": "g"}}}); err != nil {
		t.Fatal(err)
	}
	results := make(chan DifferentialTemporalJoinCompactionResult, 1)
	scheduler, err := join.StartCompactionScheduler(DifferentialTemporalJoinCompactionSchedulerOptions{
		Interval:  time.Millisecond,
		Frontiers: func(context.Context) (uint64, uint64, error) { return 5, 5, nil },
		OnResult:  func(result DifferentialTemporalJoinCompactionResult) { results <- result },
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-results:
		if result.Err != nil || result.Stats.RemovedLeft != 1 || result.Stats.RemovedRight != 1 {
			t.Fatalf("scheduler result = %#v, want one exact compaction", result)
		}
	case <-time.After(time.Second):
		t.Fatal("scheduler did not compact")
	}
	scheduler.Stop()
	scheduler.Stop()
}

func TestDifferentialTemporalJoinCompactionSchedulerRequiresOptInInputs(t *testing.T) {
	join := newMUTenSchedulerJoin(t, nil)
	if _, err := join.StartCompactionScheduler(DifferentialTemporalJoinCompactionSchedulerOptions{
		Interval: time.Millisecond,
	}); !errors.Is(err, ErrDifferentialTemporalJoinCompactionSchedulerDisabled) {
		t.Fatalf("disabled scheduler error = %v", err)
	}

	join = newMUTenSchedulerJoin(t, &DifferentialTemporalJoinCompactionPolicy{MinUpdates: 1})
	if _, err := join.StartCompactionScheduler(DifferentialTemporalJoinCompactionSchedulerOptions{
		Interval: time.Millisecond,
	}); !errors.Is(err, ErrDifferentialTemporalJoinCompactionSchedulerFrontiersRequired) {
		t.Fatalf("missing frontier provider error = %v", err)
	}
	if _, err := join.StartCompactionScheduler(DifferentialTemporalJoinCompactionSchedulerOptions{
		Interval:  -time.Millisecond,
		Frontiers: func(context.Context) (uint64, uint64, error) { return 0, 0, nil },
	}); !errors.Is(err, ErrDifferentialTemporalJoinCompactionSchedulerInvalid) {
		t.Fatalf("invalid interval error = %v", err)
	}

	scheduler, err := join.StartCompactionScheduler(DifferentialTemporalJoinCompactionSchedulerOptions{
		Interval:  time.Hour,
		Frontiers: func(context.Context) (uint64, uint64, error) { return 0, 0, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := join.StartCompactionScheduler(DifferentialTemporalJoinCompactionSchedulerOptions{
		Interval:  time.Hour,
		Frontiers: func(context.Context) (uint64, uint64, error) { return 0, 0, nil },
	}); !errors.Is(err, ErrDifferentialTemporalJoinCompactionSchedulerAlreadyRunning) {
		t.Fatalf("duplicate scheduler error = %v", err)
	}
	scheduler.Stop()
}

func TestDifferentialTemporalJoinCompactionSchedulerContextStopsAndCanRestart(t *testing.T) {
	join := newMUTenSchedulerJoin(t, &DifferentialTemporalJoinCompactionPolicy{MinUpdates: 1})
	ctx, cancel := context.WithCancel(context.Background())
	scheduler, err := join.StartCompactionScheduler(DifferentialTemporalJoinCompactionSchedulerOptions{
		Context:   ctx,
		Interval:  time.Hour,
		Frontiers: func(context.Context) (uint64, uint64, error) { return 0, 0, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-scheduler.Done():
	case <-time.After(time.Second):
		t.Fatal("scheduler did not stop after context cancellation")
	}
	restarted, err := join.StartCompactionScheduler(DifferentialTemporalJoinCompactionSchedulerOptions{
		Interval:  time.Hour,
		Frontiers: func(context.Context) (uint64, uint64, error) { return 0, 0, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	restarted.Stop()
}

func TestDifferentialTemporalJoinCompactionSchedulerReportsTransientFrontierErrors(t *testing.T) {
	join := newMUTenSchedulerJoin(t, &DifferentialTemporalJoinCompactionPolicy{MinUpdates: 1})
	if _, err := join.ApplyLeft([]DifferentialRow{{Key: "left", Time: 1, Diff: 1, Row: Row{"group": "g"}}}); err != nil {
		t.Fatal(err)
	}
	results := make(chan DifferentialTemporalJoinCompactionResult, 2)
	frontierCalls := 0
	frontierErr := errors.New("frontier unavailable")
	scheduler, err := join.StartCompactionScheduler(DifferentialTemporalJoinCompactionSchedulerOptions{
		Interval: time.Millisecond,
		Frontiers: func(context.Context) (uint64, uint64, error) {
			frontierCalls++
			if frontierCalls == 1 {
				return 0, 0, frontierErr
			}
			return 5, 5, nil
		},
		OnResult: func(result DifferentialTemporalJoinCompactionResult) { results <- result },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Stop()
	select {
	case result := <-results:
		if !errors.Is(result.Err, frontierErr) {
			t.Fatalf("frontier error result = %#v, want %v", result, frontierErr)
		}
	case <-time.After(time.Second):
		t.Fatal("scheduler did not report frontier error")
	}
	select {
	case result := <-results:
		if result.Err != nil || result.Stats.RemovedLeft != 1 {
			t.Fatalf("recovered scheduler result = %#v, want successful compaction", result)
		}
	case <-time.After(time.Second):
		t.Fatal("scheduler did not recover after frontier error")
	}
}

func newMUTenSchedulerJoin(t *testing.T, policy *DifferentialTemporalJoinCompactionPolicy) *DifferentialTemporalJoin {
	t.Helper()
	join, err := NewDifferentialTemporalJoin(DifferentialTemporalJoinDefinition{
		MaxTimeDistance:  1,
		LeftKey:          func(row SQLRow) string { return row["group"].(string) },
		RightKey:         func(row SQLRow) string { return row["group"].(string) },
		CompactionPolicy: policy,
	})
	if err != nil {
		t.Fatal(err)
	}
	return join
}
