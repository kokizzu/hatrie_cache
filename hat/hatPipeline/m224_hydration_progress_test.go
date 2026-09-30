package hatPipeline_test

import (
	"errors"
	"math"
	"testing"
	"time"

	"hatrie_cache/hat/hatPipeline"
)

func TestM224HydrationEstimateTracksProgressAndResetsPerGeneration(t *testing.T) {
	machine := hatPipeline.NewHydrationStateMachine()
	if err := machine.Begin(100); err != nil {
		t.Fatal(err)
	}
	initial := machine.Estimate()
	if initial.State != hatPipeline.HydrationStateHydrating || initial.Generation != 1 || initial.Completed != 0 || initial.Total != 100 || initial.Remaining != 100 || initial.UnitsPerSecond != 0 || initial.EstimatedRemaining != 0 {
		t.Fatalf("initial Estimate() = %#v", initial)
	}
	if err := machine.SetRate(25); err != nil {
		t.Fatalf("SetRate() error = %v", err)
	}
	first := machine.Estimate()
	if first.UnitsPerSecond != 25 || first.EstimatedRemaining != 4*time.Second {
		t.Fatalf("first Estimate() = %#v, want 25 units/s and 4s", first)
	}
	if err := machine.Advance(25); err != nil {
		t.Fatal(err)
	}
	second := machine.Estimate()
	if second.Completed != 25 || second.Remaining != 75 || second.EstimatedRemaining != 3*time.Second {
		t.Fatalf("second Estimate() = %#v, want 25 completed and 3s", second)
	}
	if err := machine.SetRate(3); err != nil {
		t.Fatal(err)
	}
	third := machine.Estimate()
	if third.EstimatedRemaining != 25*time.Second {
		t.Fatalf("third Estimate() = %#v, want 25s", third)
	}
	if err := machine.SetRate(0); err != nil {
		t.Fatal(err)
	}
	if estimate := machine.Estimate(); estimate.UnitsPerSecond != 0 || estimate.EstimatedRemaining != 0 {
		t.Fatalf("cleared Estimate() = %#v", estimate)
	}
	if err := machine.Advance(75); err != nil {
		t.Fatal(err)
	}
	if err := machine.Complete(); err != nil {
		t.Fatal(err)
	}
	if err := machine.Begin(10); err != nil {
		t.Fatal(err)
	}
	retry := machine.Estimate()
	if retry.Generation != 2 || retry.Completed != 0 || retry.Total != 10 || retry.Remaining != 10 || retry.UnitsPerSecond != 0 || retry.EstimatedRemaining != 0 {
		t.Fatalf("retry Estimate() = %#v, want rate reset", retry)
	}
}

func TestM224HydrationEstimateRoundsUpAndValidatesRates(t *testing.T) {
	machine := hatPipeline.NewHydrationStateMachine()
	if err := machine.Begin(2); err != nil {
		t.Fatal(err)
	}
	if err := machine.SetRate(3); err != nil {
		t.Fatal(err)
	}
	estimate := machine.Estimate()
	if estimate.EstimatedRemaining != 666666667*time.Nanosecond {
		t.Fatalf("rounded Estimate() = %#v, want 666666667ns", estimate)
	}
	for _, rate := range []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1)} {
		if err := machine.SetRate(rate); !errors.Is(err, hatPipeline.ErrHydrationRateInvalid) {
			t.Fatalf("SetRate(%v) error = %v, want %v", rate, err, hatPipeline.ErrHydrationRateInvalid)
		}
	}
	var nilMachine *hatPipeline.HydrationStateMachine
	if estimate := nilMachine.Estimate(); estimate != (hatPipeline.HydrationEstimate{}) {
		t.Fatalf("nil Estimate() = %#v", estimate)
	}
	if err := nilMachine.SetRate(1); !errors.Is(err, hatPipeline.ErrHydrationInvalid) {
		t.Fatalf("nil SetRate() error = %v, want %v", err, hatPipeline.ErrHydrationInvalid)
	}
}
