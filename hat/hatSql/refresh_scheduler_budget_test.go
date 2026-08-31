package hatSql_test

import (
	"context"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestManagedRefreshSchedulerCycleBudgetDefersWorkAndSetsDeadline(t *testing.T) {
	now := time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC)
	scheduler, err := hatSql.NewManagedRefreshScheduler(hatSql.ManagedRefreshSchedulerOptions{
		Now:              func() time.Time { return now },
		MaxRunsPerCycle:  1,
		MaxCycleDuration: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	calls := make([]string, 0, 2)
	deadlineSeen := false
	if err := scheduler.AddRollup("a", time.Hour, func(ctx context.Context) error {
		_, deadlineSeen = ctx.Deadline()
		calls = append(calls, "a")
		now = now.Add(2 * time.Second)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.AddRollup("b", time.Hour, func(context.Context) error {
		calls = append(calls, "b")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	runs, err := scheduler.RunDue(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || runs[0].Name != "a" || !deadlineSeen {
		t.Fatalf("first RunDue() = %#v, calls = %#v, deadline = %t", runs, calls, deadlineSeen)
	}
	runs, err = scheduler.RunDue(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || runs[0].Name != "b" || len(calls) != 2 {
		t.Fatalf("second RunDue() = %#v, calls = %#v", runs, calls)
	}
}

func TestManagedRefreshSchedulerRejectsNegativeCycleBudget(t *testing.T) {
	if _, err := hatSql.NewManagedRefreshScheduler(hatSql.ManagedRefreshSchedulerOptions{MaxRunsPerCycle: -1}); err == nil {
		t.Fatal("NewManagedRefreshScheduler() unexpectedly accepted a negative run budget")
	}
	if _, err := hatSql.NewManagedRefreshScheduler(hatSql.ManagedRefreshSchedulerOptions{MaxCycleDuration: -time.Second}); err == nil {
		t.Fatal("NewManagedRefreshScheduler() unexpectedly accepted a negative duration budget")
	}
}
