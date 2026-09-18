package hatPipeline

import (
	"errors"
	"reflect"
	"sort"
	"testing"
)

func TestMZ047TimelineRecoveryPlansReplayAdoptAndQuarantine(t *testing.T) {
	persisted := []TimelineRecoveryCheckpoint{
		{ID: "gamma", Lower: 15, Upper: 30, Generation: 5},
		{ID: "alpha", Lower: 10, Upper: 20, Generation: 4},
		{ID: "beta", Lower: 12, Upper: 24, Generation: 7},
	}
	observed := []TimelineRecoveryCheckpoint{
		{ID: "beta", Lower: 12, Upper: 24, Generation: 7},
		{ID: "gamma", Lower: 16, Upper: 31, Generation: 6},
		{ID: "alpha", Lower: 8, Upper: 18, Generation: 3},
	}

	plan, err := ReconcileTimelineRecovery(persisted, observed, TimelineRecoveryOptions{})
	if err != nil {
		t.Fatalf("ReconcileTimelineRecovery() error = %v", err)
	}
	if plan.SafeLower != 8 || plan.SafeUpper != 18 {
		t.Fatalf("safe bounds = [%d,%d], want [8,18]", plan.SafeLower, plan.SafeUpper)
	}
	if got := len(plan.Decisions); got != 3 {
		t.Fatalf("decision count = %d, want 3", got)
	}

	wantIDs := []string{"alpha", "beta", "gamma"}
	wantActions := []TimelineRecoveryAction{
		TimelineRecoveryReplay,
		TimelineRecoveryAdopt,
		TimelineRecoveryQuarantine,
	}
	for index, decision := range plan.Decisions {
		if decision.ID != wantIDs[index] || decision.Action != wantActions[index] {
			t.Errorf("decision[%d] = %#v, want id %q action %v", index, decision, wantIDs[index], wantActions[index])
		}
	}
	if plan.Decisions[0].ReplayFrom != 8 || plan.Decisions[0].ReplayThrough != 10 {
		t.Errorf("replay range = [%d,%d], want [8,10]", plan.Decisions[0].ReplayFrom, plan.Decisions[0].ReplayThrough)
	}
	if plan.Decisions[1].ReplayFrom != 0 || plan.Decisions[1].ReplayThrough != 0 {
		t.Errorf("adopt replay range = [%d,%d], want [0,0]", plan.Decisions[1].ReplayFrom, plan.Decisions[1].ReplayThrough)
	}
}

func TestMZ047TimelineRecoveryIsDeterministicAndDetached(t *testing.T) {
	persisted := []TimelineRecoveryCheckpoint{
		{ID: "zeta", Lower: 3, Upper: 8, Generation: 1},
		{ID: "alpha", Lower: 5, Upper: 9, Generation: 2},
	}
	observed := []TimelineRecoveryCheckpoint{
		{ID: "alpha", Lower: 5, Upper: 9, Generation: 2},
		{ID: "zeta", Lower: 2, Upper: 7, Generation: 1},
	}

	first, err := ReconcileTimelineRecovery(persisted, observed, TimelineRecoveryOptions{})
	if err != nil {
		t.Fatalf("first reconciliation error = %v", err)
	}
	reversed := append([]TimelineRecoveryCheckpoint(nil), persisted...)
	for left, right := 0, len(reversed)-1; left < right; left, right = left+1, right-1 {
		reversed[left], reversed[right] = reversed[right], reversed[left]
	}
	second, err := ReconcileTimelineRecovery(reversed, observed, TimelineRecoveryOptions{})
	if err != nil {
		t.Fatalf("second reconciliation error = %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("plans differ for input order: first = %#v, second = %#v", first, second)
	}

	first.Decisions[0].Persisted.ID = "mutated"
	if persisted[1].ID == "mutated" {
		t.Fatal("plan retained caller-owned persisted checkpoint storage")
	}
}

func TestMZ047TimelineRecoveryRejectsInvalidInputs(t *testing.T) {
	valid := []TimelineRecoveryCheckpoint{{ID: "events", Lower: 1, Upper: 2, Generation: 1}}
	cases := []struct {
		name      string
		persisted []TimelineRecoveryCheckpoint
		observed  []TimelineRecoveryCheckpoint
		options   TimelineRecoveryOptions
		want      error
	}{
		{
			name:      "duplicate persisted id",
			persisted: append(append([]TimelineRecoveryCheckpoint(nil), valid...), valid[0]),
			observed:  valid,
			want:      ErrTimelineRecoveryDuplicateID,
		},
		{
			name:      "missing observed id",
			persisted: valid,
			observed:  nil,
			want:      ErrTimelineRecoveryComponentMismatch,
		},
		{
			name: "invalid checkpoint bounds",
			persisted: []TimelineRecoveryCheckpoint{{
				ID: "events", Lower: 3, Upper: 2,
			}},
			observed: valid,
			want:     ErrTimelineRecoveryCheckpointInvalid,
		},
		{
			name:      "negative limit",
			persisted: valid,
			observed:  valid,
			options:   TimelineRecoveryOptions{MaxComponents: -1},
			want:      ErrTimelineRecoveryOptionsInvalid,
		},
		{
			name: "component limit",
			persisted: []TimelineRecoveryCheckpoint{
				{ID: "alpha", Lower: 1, Upper: 2},
				{ID: "beta", Lower: 1, Upper: 2},
			},
			observed: []TimelineRecoveryCheckpoint{
				{ID: "alpha", Lower: 1, Upper: 2},
				{ID: "beta", Lower: 1, Upper: 2},
			},
			options: TimelineRecoveryOptions{MaxComponents: 1},
			want:    ErrTimelineRecoveryComponentLimit,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := ReconcileTimelineRecovery(testCase.persisted, testCase.observed, testCase.options)
			if !errors.Is(err, testCase.want) {
				t.Fatalf("error = %v, want errors.Is(_, %v)", err, testCase.want)
			}
		})
	}

	_, err := ReconcileTimelineRecovery(valid, []TimelineRecoveryCheckpoint{{ID: "other", Lower: 1, Upper: 2}}, TimelineRecoveryOptions{})
	if !errors.Is(err, ErrTimelineRecoveryComponentMismatch) {
		t.Fatalf("unexpected component mismatch error = %v", err)
	}
}

func TestMZ047TimelineRecoveryQuarantinesMixedProgress(t *testing.T) {
	persisted := []TimelineRecoveryCheckpoint{{ID: "events", Lower: 10, Upper: 20, Generation: 4}}
	observed := []TimelineRecoveryCheckpoint{{ID: "events", Lower: 8, Upper: 25, Generation: 3}}
	plan, err := ReconcileTimelineRecovery(persisted, observed, TimelineRecoveryOptions{})
	if err != nil {
		t.Fatalf("ReconcileTimelineRecovery() error = %v", err)
	}
	if len(plan.Decisions) != 1 || plan.Decisions[0].Action != TimelineRecoveryQuarantine {
		t.Fatalf("mixed progress plan = %#v, want quarantine", plan)
	}
}

var mz047PlanSink TimelineRecoveryPlan

func BenchmarkMZ047TimelineRecovery(b *testing.B) {
	persisted, observed := benchmarkMZ047Inputs()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		plan, err := ReconcileTimelineRecovery(persisted, observed, TimelineRecoveryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		mz047PlanSink = plan
	}
}

// BenchmarkMZ047ManualLinearReconciliation represents the pre-MZ047 caller-side
// approach: scan observed checkpoints for every persisted component and sort
// the resulting decisions. It is a correctness baseline, not a promise that
// callers must use this slower implementation.
func BenchmarkMZ047ManualLinearReconciliation(b *testing.B) {
	persisted, observed := benchmarkMZ047Inputs()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		mz047PlanSink = benchmarkMZ047ManualPlan(persisted, observed)
	}
}

func benchmarkMZ047Inputs() ([]TimelineRecoveryCheckpoint, []TimelineRecoveryCheckpoint) {
	persisted := make([]TimelineRecoveryCheckpoint, 128)
	observed := make([]TimelineRecoveryCheckpoint, 128)
	for index := range persisted {
		id := "component-" + benchmarkMZ047Decimal(index)
		persisted[index] = TimelineRecoveryCheckpoint{ID: id, Lower: uint64(index), Upper: uint64(index + 100), Generation: 4}
		observed[index] = TimelineRecoveryCheckpoint{ID: id, Lower: uint64(index), Upper: uint64(index + 100), Generation: 4}
	}
	return persisted, observed
}

func benchmarkMZ047ManualPlan(persisted, observed []TimelineRecoveryCheckpoint) TimelineRecoveryPlan {
	plan := TimelineRecoveryPlan{Decisions: make([]TimelineRecoveryDecision, 0, len(persisted))}
	for index, persistedCheckpoint := range persisted {
		for _, observedCheckpoint := range observed {
			if observedCheckpoint.ID != persistedCheckpoint.ID {
				continue
			}
			decision := TimelineRecoveryDecision{
				ID:        persistedCheckpoint.ID,
				Persisted: persistedCheckpoint,
				Observed:  observedCheckpoint,
				Action:    benchmarkMZ047ManualAction(persistedCheckpoint, observedCheckpoint),
			}
			if decision.Action == TimelineRecoveryReplay {
				decision.ReplayFrom = observedCheckpoint.Lower
				decision.ReplayThrough = persistedCheckpoint.Lower
			}
			plan.Decisions = append(plan.Decisions, decision)
			if index == 0 {
				plan.SafeLower = minUint64(persistedCheckpoint.Lower, observedCheckpoint.Lower)
				plan.SafeUpper = minUint64(persistedCheckpoint.Upper, observedCheckpoint.Upper)
			} else {
				plan.SafeLower = minUint64(plan.SafeLower, minUint64(persistedCheckpoint.Lower, observedCheckpoint.Lower))
				plan.SafeUpper = minUint64(plan.SafeUpper, minUint64(persistedCheckpoint.Upper, observedCheckpoint.Upper))
			}
			break
		}
	}
	sort.Slice(plan.Decisions, func(left, right int) bool { return plan.Decisions[left].ID < plan.Decisions[right].ID })
	return plan
}

func benchmarkMZ047ManualAction(persisted, observed TimelineRecoveryCheckpoint) TimelineRecoveryAction {
	if observed.Lower > persisted.Lower || observed.Upper > persisted.Upper || observed.Generation > persisted.Generation {
		return TimelineRecoveryQuarantine
	}
	if observed.Lower < persisted.Lower || observed.Upper < persisted.Upper || observed.Generation < persisted.Generation {
		return TimelineRecoveryReplay
	}
	return TimelineRecoveryAdopt
}

func benchmarkMZ047Decimal(value int) string {
	if value == 0 {
		return "0"
	}
	var digits [20]byte
	position := len(digits)
	for value > 0 {
		position--
		digits[position] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[position:])
}
