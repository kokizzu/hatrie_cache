package hatSql

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestC233CPUTimeBudgetCancelsAtCooperativeCheckpoint(t *testing.T) {
	control, cancel, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{MaxCPUTime: 10 * time.Nanosecond, CPUTimeCheckEvery: 1})
	if err != nil {
		t.Fatalf("newSQLExecutionControl() error = %v", err)
	}
	defer cancel()

	var now time.Duration
	control.cpuClock = func() (int64, time.Duration, error) {
		now += 4 * time.Nanosecond
		return 11, now, nil
	}
	if err := control.check(); err != nil {
		t.Fatalf("first CPU check() error = %v", err)
	}
	if err := control.check(); err != nil {
		t.Fatalf("second CPU check() error = %v", err)
	}
	if err := control.check(); err != nil {
		t.Fatalf("third CPU check() error = %v", err)
	}
	if err := control.check(); !errors.Is(err, ErrSQLCPUTimeExceeded) {
		t.Fatalf("fourth CPU check() error = %v, want ErrSQLCPUTimeExceeded", err)
	}
	if err := control.check(); !errors.Is(err, context.Canceled) {
		t.Fatalf("repeated CPU check() error = %v, want context.Canceled", err)
	}
}

func TestC233CPUTimeBudgetAccumulatesAcrossThreads(t *testing.T) {
	control, cancel, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{MaxCPUTime: 10 * time.Nanosecond, CPUTimeCheckEvery: 1})
	if err != nil {
		t.Fatalf("newSQLExecutionControl() error = %v", err)
	}
	defer cancel()

	samples := []struct {
		thread int64
		cpu    time.Duration
	}{
		{thread: 1, cpu: 100 * time.Nanosecond},
		{thread: 1, cpu: 105 * time.Nanosecond},
		{thread: 2, cpu: 200 * time.Nanosecond},
		{thread: 2, cpu: 207 * time.Nanosecond},
	}
	index := 0
	control.cpuClock = func() (int64, time.Duration, error) {
		sample := samples[index]
		index++
		return sample.thread, sample.cpu, nil
	}
	for range 3 {
		if err := control.check(); err != nil {
			t.Fatalf("baseline CPU check() error = %v", err)
		}
	}
	if err := control.check(); !errors.Is(err, ErrSQLCPUTimeExceeded) {
		t.Fatalf("cross-thread CPU check() error = %v, want ErrSQLCPUTimeExceeded", err)
	}
}

func TestC233CPUTimeBudgetDisabledByDefault(t *testing.T) {
	control, cancel, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{})
	if err != nil {
		t.Fatalf("newSQLExecutionControl() error = %v", err)
	}
	defer cancel()
	if err := control.check(); err != nil {
		t.Fatalf("default check() error = %v", err)
	}
}

func TestC233CPUTimeBudgetValidatesNegativeLimit(t *testing.T) {
	if _, cancel, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{MaxCPUTime: -time.Nanosecond}); err == nil {
		cancel()
		t.Fatal("negative MaxCPUTime was accepted")
	}
	if _, cancel, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{CPUTimeCheckEvery: -1}); err == nil {
		cancel()
		t.Fatal("negative CPUTimeCheckEvery was accepted")
	}
	if _, cancel, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{CPUTimeCheckEvery: MaxSQLCPUTimeCheckEvery + 1}); err == nil {
		cancel()
		t.Fatal("oversized CPUTimeCheckEvery was accepted")
	}
}
