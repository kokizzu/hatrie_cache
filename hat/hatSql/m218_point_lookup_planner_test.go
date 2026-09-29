package hatSql

import "testing"

func TestM218PointLookupPlannerChoosesBestReusableArrangement(t *testing.T) {
	candidates := []SQLPointLookupCandidate{
		{
			SQLArrangementCostCandidate: SQLArrangementCostCandidate{
				Key:            "cold",
				Field:          "region",
				Kind:           "point-lookup",
				BuildCostNanos: 1_000,
				ProbeCostNanos: 90,
				ScanCostNanos:  100,
				ExpectedReads:  1,
				ExpectedWrites: 1,
				MemoryBytes:    128,
			},
			Available: true,
		},
		{
			SQLArrangementCostCandidate: SQLArrangementCostCandidate{
				Key:              "hot",
				Field:            "region",
				Kind:             "point-lookup",
				BuildCostNanos:   100,
				ProbeCostNanos:   10,
				ScanCostNanos:    1_000,
				MaintenanceNanos: 2,
				ExpectedReads:    100,
				ExpectedWrites:   1,
				MemoryBytes:      256,
			},
			Available: true,
		},
	}
	plan := PlanSQLPointLookup(candidates, SQLPointLookupPlanOptions{MemoryBudgetBytes: 512})
	if plan.Strategy != SQLPointLookupArrangement || !plan.HasSelection {
		t.Fatalf("plan = %#v, want arrangement selection", plan)
	}
	if plan.Selected.Key != "hot" {
		t.Fatalf("selected key = %q, want hot", plan.Selected.Key)
	}
	if plan.SelectedScore.PaybackReads != 1 || plan.SelectedScore.NetBenefitNanos <= 0 {
		t.Fatalf("selected score = %#v, want positive one-read payback", plan.SelectedScore)
	}
	if len(plan.Candidates) != 2 {
		t.Fatalf("candidate scores = %d, want 2", len(plan.Candidates))
	}
	if plan.Candidates[0].Candidate.Key != "hot" || plan.Candidates[1].Reason != SQLPointLookupReasonInsufficientReads {
		t.Fatalf("candidate explanations = %#v, want hot then insufficient reads", plan.Candidates)
	}
}

func TestM218PointLookupPlannerFallsBackToScanForCostAndAvailability(t *testing.T) {
	tests := []struct {
		name       string
		candidate  SQLPointLookupCandidate
		wantReason string
	}{
		{
			name: "unavailable",
			candidate: SQLPointLookupCandidate{
				SQLArrangementCostCandidate: SQLArrangementCostCandidate{Key: "missing", ScanCostNanos: 100, ProbeCostNanos: 1, ExpectedReads: 10},
			},
			wantReason: SQLPointLookupReasonUnavailable,
		},
		{
			name: "memory",
			candidate: SQLPointLookupCandidate{
				SQLArrangementCostCandidate: SQLArrangementCostCandidate{Key: "large", ScanCostNanos: 100, ProbeCostNanos: 1, ExpectedReads: 10, MemoryBytes: 2_048},
				Available:                   true,
			},
			wantReason: SQLPointLookupReasonMemoryBudget,
		},
		{
			name: "no savings",
			candidate: SQLPointLookupCandidate{
				SQLArrangementCostCandidate: SQLArrangementCostCandidate{Key: "slow", ScanCostNanos: 100, ProbeCostNanos: 100, ExpectedReads: 10},
				Available:                   true,
			},
			wantReason: SQLPointLookupReasonNoReadSavings,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := PlanSQLPointLookup([]SQLPointLookupCandidate{test.candidate}, SQLPointLookupPlanOptions{MemoryBudgetBytes: 1_024})
			if plan.Strategy != SQLPointLookupScan || plan.HasSelection {
				t.Fatalf("plan = %#v, want scan fallback", plan)
			}
			if plan.Reason != test.wantReason || len(plan.Candidates) != 1 || plan.Candidates[0].Reason != test.wantReason {
				t.Fatalf("reason = %q candidates = %#v, want %q", plan.Reason, plan.Candidates, test.wantReason)
			}
		})
	}
}

func TestM218PointLookupPlannerIsDeterministicAndDoesNotMutateInput(t *testing.T) {
	candidates := []SQLPointLookupCandidate{
		{
			SQLArrangementCostCandidate: SQLArrangementCostCandidate{
				Key: "z", ScanCostNanos: 100, ProbeCostNanos: 10, ExpectedReads: 10,
			},
			Available: true,
		},
		{
			SQLArrangementCostCandidate: SQLArrangementCostCandidate{
				Key: "a", ScanCostNanos: 100, ProbeCostNanos: 10, ExpectedReads: 10,
			},
			Available: true,
		},
	}
	plan := PlanSQLPointLookup(candidates, SQLPointLookupPlanOptions{})
	if plan.Selected.Key != "a" || plan.Candidates[0].Candidate.Key != "a" {
		t.Fatalf("deterministic plan = %#v, want a first", plan)
	}
	if candidates[0].Key != "z" || candidates[1].Key != "a" {
		t.Fatalf("input candidates mutated = %#v", candidates)
	}
	empty := PlanSQLPointLookup(nil, SQLPointLookupPlanOptions{})
	if empty.Strategy != SQLPointLookupScan || empty.Reason != SQLPointLookupReasonNoCandidate {
		t.Fatalf("empty plan = %#v, want no-candidate scan", empty)
	}
}
