package hatSql

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestC233CPUTimeBudgetCooperativeCancellation(t *testing.T) {
	var cpu time.Duration
	clock := func() time.Duration {
		cpu += time.Millisecond
		return cpu
	}

	control, cancel, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{
		MaxCPUTime:    3 * time.Millisecond,
		CPUTimeSource: NewSQLCPUTimeSource(clock),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	for checks := 0; checks < 10; checks++ {
		if err := control.check(); err != nil {
			if !errors.Is(err, ErrSQLCPUTimeExceeded) {
				t.Fatalf("check returned %v, want ErrSQLCPUTimeExceeded", err)
			}
			return
		}
	}
	t.Fatal("CPU time budget did not cancel the query")
}

func TestC233CPUTimeBudgetValidation(t *testing.T) {
	if _, cancel, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{MaxCPUTime: -time.Nanosecond}); err == nil {
		cancel()
		t.Fatal("negative MaxCPUTime was accepted")
	}

	if _, cancel, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{MaxCPUTime: time.Nanosecond}); err == nil {
		cancel()
		t.Fatal("MaxCPUTime without CPUTimeSource was accepted")
	} else if !errors.Is(err, ErrSQLCPUTimeSourceRequired) {
		t.Fatalf("missing source returned %v, want ErrSQLCPUTimeSourceRequired", err)
	}
}

func TestC233DisabledCPUTimeBudgetDoesNotReadClock(t *testing.T) {
	calls := 0
	control, cancel, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{
		CPUTimeSource: NewSQLCPUTimeSource(func() time.Duration {
			calls++
			return time.Duration(calls)
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	if err := control.check(); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatalf("disabled CPU budget read clock %d times", calls)
	}
}

func TestC233CPUTimeBudgetRejectsZeroValueSource(t *testing.T) {
	_, cancel, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{
		MaxCPUTime:    time.Second,
		CPUTimeSource: &SQLCPUTimeSource{},
	})
	if err == nil {
		cancel()
		t.Fatal("zero-value CPUTimeSource was accepted")
	}
	if !errors.Is(err, ErrSQLCPUTimeSourceRequired) {
		t.Fatalf("zero-value source returned %v, want ErrSQLCPUTimeSourceRequired", err)
	}
}
