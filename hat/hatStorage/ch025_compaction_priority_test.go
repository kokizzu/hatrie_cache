package hatStorage_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"hatrie_cache/hat/hatStorage"
)

func TestCH025CompactionPriorityPolicyBalancesFreshnessAndSpace(t *testing.T) {
	policy := hatStorage.DefaultCompactionPriorityPolicy()
	spaceHeavy := hatStorage.CompactionPriorityMetrics{ReclaimableBytes: 16 << 20}
	freshnessHeavy := hatStorage.CompactionPriorityMetrics{FreshnessLag: 10 * time.Minute}
	if policy.Priority(spaceHeavy) <= policy.Priority(freshnessHeavy) {
		t.Fatalf("space-heavy priority = %d, freshness-heavy priority = %d, want space-heavy to win", policy.Priority(spaceHeavy), policy.Priority(freshnessHeavy))
	}
	if policy.Priority(freshnessHeavy) <= policy.Priority(hatrieStoragePriorityMetricsZero()) {
		t.Fatal("positive freshness did not produce a positive priority")
	}
}

func TestCH025CompactionSchedulerUsesOptInPriorityPolicy(t *testing.T) {
	policy := hatStorage.DefaultCompactionPriorityPolicy()
	scheduler, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{
		MaxConcurrent:  1,
		PriorityPolicy: &policy,
	})
	if err != nil {
		t.Fatalf("NewCompactionScheduler() error = %v", err)
	}
	var order []string
	record := func(name string) func(context.Context) error {
		return func(context.Context) error {
			order = append(order, name)
			return nil
		}
	}
	if _, err := scheduler.ScheduleWithPriorityMetrics("space", hatStorage.CompactionPriorityMetrics{ReclaimableBytes: 16 << 20}, record("space")); err != nil {
		t.Fatalf("ScheduleWithPriorityMetrics(space) error = %v", err)
	}
	if _, err := scheduler.ScheduleWithPriorityMetrics("fresh", hatStorage.CompactionPriorityMetrics{FreshnessLag: 10 * time.Minute}, record("fresh")); err != nil {
		t.Fatalf("ScheduleWithPriorityMetrics(fresh) error = %v", err)
	}
	if _, err := scheduler.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if want := []string{"space", "fresh"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("execution order = %v, want %v", order, want)
	}
}

func TestCH025CompactionPriorityIsDisabledByDefault(t *testing.T) {
	scheduler, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{MaxConcurrent: 1})
	if err != nil {
		t.Fatalf("NewCompactionScheduler() error = %v", err)
	}
	if _, err := scheduler.ScheduleWithPriorityMetrics("disabled", hatStorage.CompactionPriorityMetrics{FreshnessLag: time.Minute}, func(context.Context) error { return nil }); !errors.Is(err, hatStorage.ErrCompactionPriorityPolicyDisabled) {
		t.Fatalf("disabled policy error = %v, want ErrCompactionPriorityPolicyDisabled", err)
	}
}

func TestCH025CompactionPriorityPolicyValidatesUnits(t *testing.T) {
	invalid := hatStorage.CompactionPriorityPolicy{FreshnessUnit: time.Minute}
	if _, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{PriorityPolicy: &invalid}); !errors.Is(err, hatStorage.ErrCompactionPriorityPolicyInvalid) {
		t.Fatalf("invalid policy error = %v, want ErrCompactionPriorityPolicyInvalid", err)
	}
}

func hatrieStoragePriorityMetricsZero() hatStorage.CompactionPriorityMetrics {
	return hatStorage.CompactionPriorityMetrics{}
}
