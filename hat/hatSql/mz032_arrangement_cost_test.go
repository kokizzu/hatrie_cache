package hatSql

import (
	"fmt"
	"testing"
)

func TestSQLArrangementCostModelEvaluatesPaybackAndMemory(t *testing.T) {
	model := NewSQLArrangementCostModel(SQLArrangementCostModelOptions{MemoryBudgetBytes: 1024})
	score := model.Evaluate(SQLArrangementCostCandidate{
		Key:              "users",
		Field:            "region",
		Kind:             "hash",
		BuildCostNanos:   100,
		ProbeCostNanos:   10,
		ScanCostNanos:    110,
		MaintenanceNanos: 5,
		ExpectedReads:    20,
		ExpectedWrites:   2,
		MemoryBytes:      512,
	})
	if score.ReadSavingsNanos != 100 {
		t.Fatalf("read savings = %d, want 100", score.ReadSavingsNanos)
	}
	if score.TotalCostNanos != 110 {
		t.Fatalf("total cost = %d, want 110", score.TotalCostNanos)
	}
	if score.ExpectedBenefitNanos != 2000 {
		t.Fatalf("expected benefit = %d, want 2000", score.ExpectedBenefitNanos)
	}
	if score.NetBenefitNanos != 1890 {
		t.Fatalf("net benefit = %d, want 1890", score.NetBenefitNanos)
	}
	if score.PaybackReads != 2 {
		t.Fatalf("payback reads = %d, want 2", score.PaybackReads)
	}
	if !score.FitsMemory || !score.Reusable {
		t.Fatalf("score = %+v, want a reusable in-budget arrangement", score)
	}
}

func TestSQLArrangementCostModelRejectsNoSavingsAndMemoryOverflow(t *testing.T) {
	model := NewSQLArrangementCostModel(SQLArrangementCostModelOptions{MemoryBudgetBytes: 1024})
	noSavings := model.Evaluate(SQLArrangementCostCandidate{
		BuildCostNanos: 100,
		ProbeCostNanos: 100,
		ScanCostNanos:  100,
		ExpectedReads:  100,
		MemoryBytes:    1,
	})
	if noSavings.PaybackReads != 0 || noSavings.Reusable || noSavings.NetBenefitNanos != -100 {
		t.Fatalf("no-savings score = %+v, want no payback and negative net benefit", noSavings)
	}

	overBudget := model.Evaluate(SQLArrangementCostCandidate{
		BuildCostNanos: 1,
		ProbeCostNanos: 1,
		ScanCostNanos:  11,
		ExpectedReads:  10,
		MemoryBytes:    1025,
	})
	if overBudget.FitsMemory || overBudget.Reusable {
		t.Fatalf("over-budget score = %+v, want rejected", overBudget)
	}

	free := model.Evaluate(SQLArrangementCostCandidate{
		ProbeCostNanos: 1,
		ScanCostNanos:  2,
		ExpectedReads:  1,
		MemoryBytes:    1,
	})
	if free.PaybackReads != 0 || !free.Reusable {
		t.Fatalf("free score = %+v, want immediate reusable arrangement", free)
	}
}

func TestSQLArrangementCostModelRankIsDeterministic(t *testing.T) {
	model := NewSQLArrangementCostModel(SQLArrangementCostModelOptions{})
	candidates := []SQLArrangementCostCandidate{
		{Key: "z", BuildCostNanos: 1, ProbeCostNanos: 1, ScanCostNanos: 2, ExpectedReads: 10},
		{Key: "a", BuildCostNanos: 1, ProbeCostNanos: 1, ScanCostNanos: 3, ExpectedReads: 10},
		{Key: "b", BuildCostNanos: 1, ProbeCostNanos: 2, ScanCostNanos: 1, ExpectedReads: 10},
	}
	ranked := model.Rank(candidates)
	if len(ranked) != len(candidates) {
		t.Fatalf("ranked length = %d, want %d", len(ranked), len(candidates))
	}
	if ranked[0].Candidate.Key != "a" || ranked[1].Candidate.Key != "z" || ranked[2].Candidate.Key != "b" {
		t.Fatalf("ranked keys = %q, %q, %q; want a, z, b", ranked[0].Candidate.Key, ranked[1].Candidate.Key, ranked[2].Candidate.Key)
	}

	buffer := make([]SQLArrangementCostScore, 0, len(candidates))
	reused := model.RankInto(buffer, candidates)
	if len(reused) != len(candidates) || &reused[0] != &buffer[:len(candidates)][0] {
		t.Fatal("RankInto did not reuse the supplied buffer")
	}
}

func TestSQLArrangementCostModelSaturatesArithmetic(t *testing.T) {
	model := NewSQLArrangementCostModel(SQLArrangementCostModelOptions{})
	score := model.Evaluate(SQLArrangementCostCandidate{
		BuildCostNanos:   ^uint64(0),
		MaintenanceNanos: ^uint64(0),
		ExpectedWrites:   ^uint64(0),
		ProbeCostNanos:   1,
		ScanCostNanos:    ^uint64(0),
		ExpectedReads:    ^uint64(0),
		MemoryBytes:      1,
	})
	if score.TotalCostNanos != ^uint64(0) || score.ExpectedBenefitNanos != ^uint64(0) {
		t.Fatalf("saturated score = %+v, want saturated totals", score)
	}
	if score.NetBenefitNanos != 0 {
		t.Fatalf("net benefit = %d, want 0 after equal saturation", score.NetBenefitNanos)
	}
}

func TestSQLArrangementCostModelEvaluationDoesNotAllocate(t *testing.T) {
	model := NewSQLArrangementCostModel(SQLArrangementCostModelOptions{})
	candidate := SQLArrangementCostCandidate{
		BuildCostNanos: 100,
		ProbeCostNanos: 10,
		ScanCostNanos:  110,
		ExpectedReads:  20,
		MemoryBytes:    32,
	}
	var score SQLArrangementCostScore
	allocs := testing.AllocsPerRun(100, func() {
		score = model.Evaluate(candidate)
	})
	if allocs != 0 {
		t.Fatalf("Evaluate allocations = %f, want 0 (score=%+v)", allocs, score)
	}
}

func TestSQLArrangementCostModelMatchesBenchmarkFormula(t *testing.T) {
	model := NewSQLArrangementCostModel(SQLArrangementCostModelOptions{MemoryBudgetBytes: 1 << 20})
	for index, candidate := range mz032BenchmarkCandidates() {
		got := model.Evaluate(candidate)
		want := mz032ManualArrangementScore(candidate)
		if got != want {
			t.Fatalf("candidate %d score = %+v, want %+v", index, got, want)
		}
	}
}

func ExampleSQLArrangementCostModel() {
	model := NewSQLArrangementCostModel(SQLArrangementCostModelOptions{MemoryBudgetBytes: 1024})
	ranked := model.Rank([]SQLArrangementCostCandidate{{
		Key:              "users",
		Field:            "region",
		BuildCostNanos:   100,
		ProbeCostNanos:   10,
		ScanCostNanos:    110,
		MaintenanceNanos: 5,
		ExpectedReads:    20,
		ExpectedWrites:   2,
		MemoryBytes:      512,
	}})
	fmt.Printf("%s.%s reusable=%t payback=%d net=%d\n",
		ranked[0].Candidate.Key,
		ranked[0].Candidate.Field,
		ranked[0].Reusable,
		ranked[0].PaybackReads,
		ranked[0].NetBenefitNanos,
	)
	// Output:
	// users.region reusable=true payback=2 net=1890
}
