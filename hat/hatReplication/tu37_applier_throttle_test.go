//go:build !tu37baseline

package hatReplication

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestApplierThrottleReservesBurstAndPacesBatches(t *testing.T) {
	now := time.Unix(100, 0)
	throttle, err := NewApplierThrottle(ApplierThrottleOptions{
		EntriesPerSecond: 100,
		Burst:            2,
		Now:              func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewApplierThrottle() error = %v", err)
	}
	if delay, err := throttle.Reserve(2); err != nil || delay != 0 {
		t.Fatalf("Reserve(burst) = %v/%v, want 0/nil", delay, err)
	}
	if delay, err := throttle.Reserve(1); err != nil || delay != 10*time.Millisecond {
		t.Fatalf("Reserve(after burst) = %v/%v, want 10ms/nil", delay, err)
	}
	now = now.Add(20 * time.Millisecond)
	if delay, err := throttle.Reserve(1); err != nil || delay != 0 {
		t.Fatalf("Reserve(after refill) = %v/%v, want 0/nil", delay, err)
	}
}

func TestApplierThrottleSupportsLargeBatchesAndContextCancellation(t *testing.T) {
	now := time.Unix(200, 0)
	throttle, err := NewApplierThrottle(ApplierThrottleOptions{
		EntriesPerSecond: 10,
		Burst:            2,
		Now:              func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewApplierThrottle() error = %v", err)
	}
	if delay, err := throttle.Reserve(3); err != nil || delay != 100*time.Millisecond {
		t.Fatalf("Reserve(large batch) = %v/%v, want 100ms/nil", delay, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := throttle.Wait(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait(canceled) error = %v, want context canceled", err)
	}
}

func TestApplierThrottleRejectsInvalidConfiguration(t *testing.T) {
	cases := []ApplierThrottleOptions{
		{},
		{EntriesPerSecond: -1, Burst: 1},
		{EntriesPerSecond: 1, Burst: 0},
	}
	for _, options := range cases {
		if _, err := NewApplierThrottle(options); !errors.Is(err, ErrApplierThrottleInvalidOptions) {
			t.Fatalf("NewApplierThrottle(%+v) error = %v, want invalid options", options, err)
		}
	}
}

func TestApplierThrottleRejectsInvalidReservations(t *testing.T) {
	throttle, err := NewApplierThrottle(ApplierThrottleOptions{EntriesPerSecond: 1, Burst: 1})
	if err != nil {
		t.Fatalf("NewApplierThrottle() error = %v", err)
	}
	if _, err := throttle.Reserve(0); !errors.Is(err, ErrApplierThrottleInvalidEntries) {
		t.Fatalf("Reserve(0) error = %v, want invalid entries", err)
	}
	if err := throttle.Wait(context.Background(), -1); !errors.Is(err, ErrApplierThrottleInvalidEntries) {
		t.Fatalf("Wait(-1) error = %v, want invalid entries", err)
	}
}
