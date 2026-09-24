package hatFiber

import (
	"context"
	"errors"
	"testing"
)

func TestTT033TenantQuotaRejectsExcessFibersAndReusesAfterReap(t *testing.T) {
	scheduler, err := New(Options{
		MaxFibers: 4,
		TenantQuotas: map[string]TenantQuota{
			"api": {MaxFibers: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	done := func(context.Context) (Step, error) { return StepDone, nil }
	first, err := scheduler.SpawnForTenant("api", done)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scheduler.SpawnForTenant("api", done); !errors.Is(err, ErrTenantQuotaExceeded) {
		t.Fatalf("second tenant spawn error = %v, want quota exceeded", err)
	}
	if _, err := scheduler.SpawnForTenant("worker", done); err != nil {
		t.Fatalf("unconfigured tenant spawn error = %v", err)
	}
	if _, err := scheduler.Run(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Reap(first); err != nil {
		t.Fatal(err)
	}
	if _, err := scheduler.SpawnForTenant("api", done); err != nil {
		t.Fatalf("tenant spawn after reap error = %v", err)
	}
}

func TestTT033TenantQuotaStepBudgetPreservesFairness(t *testing.T) {
	scheduler, err := New(Options{
		MaxFibers: 4,
		TenantQuotas: map[string]TenantQuota{
			"api": {MaxStepsPerRun: 2},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	apiSteps := 0
	api, err := scheduler.SpawnForTenant("api", func(context.Context) (Step, error) {
		apiSteps++
		if apiSteps == 4 {
			return StepDone, nil
		}
		return StepYield, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	workerSteps := 0
	worker, err := scheduler.SpawnForTenant("worker", func(context.Context) (Step, error) {
		workerSteps++
		return StepDone, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	firstRun, err := scheduler.Run(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if firstRun.Steps != 3 || firstRun.Completed != 1 || firstRun.Remaining != 1 || firstRun.Throttled == 0 {
		t.Fatalf("first run = %#v, want two API steps, one worker completion, and throttling", firstRun)
	}
	if workerSteps != 1 || apiSteps != 2 {
		t.Fatalf("step counts after first run = api:%d worker:%d", apiSteps, workerSteps)
	}
	secondRun, err := scheduler.Run(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if secondRun.Completed != 1 || apiSteps != 4 {
		t.Fatalf("second run = %#v api_steps=%d, want API completion", secondRun, apiSteps)
	}
	if err := scheduler.Reap(api); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Reap(worker); err != nil {
		t.Fatal(err)
	}
}

func TestTT033TenantQuotaCancellationRequiresReapBeforeReuse(t *testing.T) {
	scheduler, err := New(Options{
		MaxFibers: 2,
		TenantQuotas: map[string]TenantQuota{
			"api": {MaxFibers: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	id, err := scheduler.SpawnForTenant("api", func(context.Context) (Step, error) {
		return StepYield, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Cancel(id); err != nil {
		t.Fatal(err)
	}
	if _, err := scheduler.SpawnForTenant("api", func(context.Context) (Step, error) {
		return StepDone, nil
	}); !errors.Is(err, ErrTenantQuotaExceeded) {
		t.Fatalf("spawn before reap error = %v, want quota exceeded", err)
	}
	if err := scheduler.Reap(id); err != nil {
		t.Fatal(err)
	}
	if _, err := scheduler.SpawnForTenant("api", func(context.Context) (Step, error) {
		return StepDone, nil
	}); err != nil {
		t.Fatalf("spawn after canceled fiber reap error = %v", err)
	}
}

func TestTT033TenantQuotaRejectsInvalidConfiguration(t *testing.T) {
	if _, err := New(Options{TenantQuotas: map[string]TenantQuota{"": {MaxFibers: 1}}}); !errors.Is(err, ErrTenantNameInvalid) {
		t.Fatalf("empty tenant error = %v", err)
	}
	if _, err := New(Options{TenantQuotas: map[string]TenantQuota{"api": {MaxFibers: -1}}}); !errors.Is(err, ErrTenantQuotaInvalid) {
		t.Fatalf("negative quota error = %v", err)
	}
}
