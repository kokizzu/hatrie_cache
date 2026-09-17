package hatSql

import (
	"fmt"
	"sync"
	"testing"
)

func mz033TestCandidate(key string, build, reads uint64) SQLArrangementCostCandidate {
	return SQLArrangementCostCandidate{
		Key:              key,
		Field:            "region",
		Kind:             "hash",
		BuildCostNanos:   build,
		ProbeCostNanos:   10,
		ScanCostNanos:    110,
		MaintenanceNanos: 1,
		ExpectedReads:    reads,
		ExpectedWrites:   1,
		MemoryBytes:      512,
	}
}

func TestSQLDataflowIndexAdvisorAggregatesAndRanksRecommendations(t *testing.T) {
	advisor := NewSQLDataflowIndexAdvisor(SQLDataflowIndexAdvisorOptions{
		Capacity:   4,
		MinSamples: 2,
		CostModel:  SQLArrangementCostModelOptions{MemoryBudgetBytes: 1024},
	})
	if !advisor.ObserveDataflow(SQLDataflowIndexObservation{Candidate: mz033TestCandidate("slow", 100, 10)}) {
		t.Fatal("first observation was rejected")
	}
	if !advisor.ObserveDataflow(SQLDataflowIndexObservation{Candidate: mz033TestCandidate("slow", 100, 10)}) {
		t.Fatal("second observation was rejected")
	}
	if recommendations := advisor.Recommendations(0); len(recommendations) != 1 {
		t.Fatalf("recommendations after aggregation = %d, want 1", len(recommendations))
	} else {
		recommendation := recommendations[0]
		if recommendation.Candidate.ExpectedReads != 20 || recommendation.Candidate.ExpectedWrites != 2 {
			t.Fatalf("aggregated candidate = %+v, want 20 reads and 2 writes", recommendation.Candidate)
		}
		if recommendation.Samples != 2 || !recommendation.Reusable || recommendation.PaybackReads != 2 {
			t.Fatalf("recommendation = %+v, want two-sample reusable candidate", recommendation)
		}
	}

	if !advisor.ObserveDataflow(SQLDataflowIndexObservation{Candidate: mz033TestCandidate("fast", 10, 10)}) {
		t.Fatal("fast first observation was rejected")
	}
	if !advisor.ObserveDataflow(SQLDataflowIndexObservation{Candidate: mz033TestCandidate("fast", 10, 10)}) {
		t.Fatal("fast second observation was rejected")
	}
	recommendations := advisor.Recommendations(1)
	if len(recommendations) != 1 || recommendations[0].Candidate.Key != "fast" {
		t.Fatalf("limited recommendations = %+v, want fast first", recommendations)
	}
}

func TestSQLDataflowIndexAdvisorRequiresSamplesAndRejectsBudget(t *testing.T) {
	advisor := NewSQLDataflowIndexAdvisor(SQLDataflowIndexAdvisorOptions{
		Capacity:   4,
		MinSamples: 2,
		CostModel:  SQLArrangementCostModelOptions{MemoryBudgetBytes: 1024},
	})
	candidate := mz033TestCandidate("sampled", 10, 10)
	if !advisor.ObserveDataflow(SQLDataflowIndexObservation{Candidate: candidate}) {
		t.Fatal("observation was rejected")
	}
	if recommendations := advisor.Recommendations(0); len(recommendations) != 0 {
		t.Fatalf("one-sample recommendations = %+v, want none", recommendations)
	}

	overBudget := candidate
	overBudget.Key = "over_budget"
	overBudget.MemoryBytes = 1025
	if !advisor.ObserveDataflow(SQLDataflowIndexObservation{Candidate: overBudget}) ||
		!advisor.ObserveDataflow(SQLDataflowIndexObservation{Candidate: overBudget}) {
		t.Fatal("over-budget observations were rejected")
	}
	if recommendations := advisor.Recommendations(0); len(recommendations) != 0 {
		t.Fatalf("over-budget recommendations = %+v, want none", recommendations)
	}
}

func TestSQLDataflowIndexAdvisorBoundsCandidatesAndValidatesIdentity(t *testing.T) {
	advisor := NewSQLDataflowIndexAdvisor(SQLDataflowIndexAdvisorOptions{Capacity: 1, MinSamples: 1})
	if advisor.ObserveDataflow(SQLDataflowIndexObservation{Candidate: SQLArrangementCostCandidate{Field: "region"}}) {
		t.Fatal("empty-key observation was accepted")
	}
	if advisor.ObserveDataflow(SQLDataflowIndexObservation{Candidate: SQLArrangementCostCandidate{Key: "source"}}) {
		t.Fatal("empty-field observation was accepted")
	}
	first := mz033TestCandidate("first", 10, 10)
	second := mz033TestCandidate("second", 10, 10)
	if !advisor.ObserveDataflow(SQLDataflowIndexObservation{Candidate: first}) {
		t.Fatal("first bounded candidate was rejected")
	}
	if advisor.ObserveDataflow(SQLDataflowIndexObservation{Candidate: second}) {
		t.Fatal("second bounded candidate was accepted")
	}
	if !advisor.ObserveDataflow(SQLDataflowIndexObservation{Candidate: first}) {
		t.Fatal("existing bounded candidate was rejected")
	}
}

func TestSQLDataflowIndexAdvisorResetClearsObservations(t *testing.T) {
	advisor := NewSQLDataflowIndexAdvisor(SQLDataflowIndexAdvisorOptions{Capacity: 1, MinSamples: 1})
	if !advisor.ObserveDataflow(SQLDataflowIndexObservation{Candidate: mz033TestCandidate("reset", 10, 10)}) {
		t.Fatal("initial observation was rejected")
	}
	if len(advisor.Recommendations(0)) != 1 {
		t.Fatal("initial recommendation was not retained")
	}
	advisor.Reset()
	if recommendations := advisor.Recommendations(0); len(recommendations) != 0 {
		t.Fatalf("recommendations after reset = %+v, want none", recommendations)
	}
	if !advisor.ObserveDataflow(SQLDataflowIndexObservation{Candidate: mz033TestCandidate("reset", 10, 10)}) {
		t.Fatal("observation after reset was rejected")
	}
	if len(advisor.Recommendations(0)) != 1 {
		t.Fatal("recommendation after reset was not retained")
	}
}

func TestSQLDataflowIndexAdvisorObservesBatches(t *testing.T) {
	advisor := NewSQLDataflowIndexAdvisor(SQLDataflowIndexAdvisorOptions{Capacity: 2, MinSamples: 1})
	first := mz033TestCandidate("batch_first", 10, 10)
	second := mz033TestCandidate("batch_second", 10, 10)
	accepted := advisor.ObserveDataflows([]SQLDataflowIndexObservation{
		{Candidate: first},
		{Candidate: second},
		{Candidate: mz033TestCandidate("batch_third", 10, 10)},
		{Candidate: first},
	})
	if accepted != 3 {
		t.Fatalf("accepted batch observations = %d, want 3", accepted)
	}
	recommendations := advisor.Recommendations(0)
	if len(recommendations) != 2 {
		t.Fatalf("batch recommendations = %d, want 2", len(recommendations))
	}
	for _, recommendation := range recommendations {
		if recommendation.Samples != 1 && recommendation.Samples != 2 {
			t.Fatalf("batch recommendation = %+v, want one or two samples", recommendation)
		}
	}
}

func TestSQLDataflowIndexAdvisorDefaultsAndConcurrentObservation(t *testing.T) {
	advisor := NewSQLDataflowIndexAdvisor(SQLDataflowIndexAdvisorOptions{})
	candidate := mz033TestCandidate("concurrent", 10, 1)
	const observations = 32
	var group sync.WaitGroup
	for index := 0; index < observations; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			if !advisor.ObserveDataflow(SQLDataflowIndexObservation{Candidate: candidate}) {
				t.Errorf("concurrent observation was rejected")
			}
		}()
	}
	group.Wait()
	recommendations := advisor.Recommendations(1)
	if len(recommendations) != 1 {
		t.Fatalf("default recommendations = %d, want 1", len(recommendations))
	}
	if recommendations[0].Samples != observations || recommendations[0].Candidate.ExpectedReads != observations {
		t.Fatalf("concurrent recommendation = %+v, want %d samples and reads", recommendations[0], observations)
	}
}

func TestSQLDataflowIndexAdvisorMatchesManualRecommendation(t *testing.T) {
	observations := mz033BenchmarkObservations()
	expectedSamples := make(map[mz033ManualKey]uint64, 512)
	for _, observation := range observations {
		expectedSamples[mz033ManualKey{key: observation.key, field: observation.field, kind: observation.kind}]++
	}
	model := NewSQLArrangementCostModel(SQLArrangementCostModelOptions{MemoryBudgetBytes: 4096})
	manual := mz033ManualAdvisor{
		capacity: 512,
		minimum:  2,
		model:    model,
		stats:    make(map[mz033ManualKey]mz033ManualStats, 512),
	}
	advisor := NewSQLDataflowIndexAdvisor(SQLDataflowIndexAdvisorOptions{
		Capacity:   512,
		MinSamples: 2,
		CostModel:  SQLArrangementCostModelOptions{MemoryBudgetBytes: 4096},
	})
	for index, observation := range observations {
		manual.observe(observation)
		sqlObservation := SQLDataflowIndexObservation{
			Candidate: SQLArrangementCostCandidate{
				Key:              observation.key,
				Field:            observation.field,
				Kind:             observation.kind,
				BuildCostNanos:   observation.buildNanos,
				ProbeCostNanos:   observation.probeNanos,
				ScanCostNanos:    observation.scanNanos,
				MaintenanceNanos: observation.maintenanceNanos,
				ExpectedReads:    observation.reads,
				ExpectedWrites:   observation.writes,
				MemoryBytes:      observation.memoryBytes,
			},
		}
		if !advisor.ObserveDataflow(sqlObservation) {
			t.Fatalf("observation %d was rejected", index)
		}
	}
	manualScores := manual.recommendations()
	recommendations := advisor.Recommendations(0)
	if len(recommendations) != len(manualScores) {
		t.Fatalf("recommendation count = %d, want %d", len(recommendations), len(manualScores))
	}
	for index, recommendation := range recommendations {
		if recommendation.SQLArrangementCostScore != manualScores[index] {
			t.Fatalf("recommendation %d = %+v, want %+v", index, recommendation.SQLArrangementCostScore, manualScores[index])
		}
		key := mz033ManualKey{
			key:   recommendation.Candidate.Key,
			field: recommendation.Candidate.Field,
			kind:  recommendation.Candidate.Kind,
		}
		if recommendation.Samples != expectedSamples[key] {
			t.Fatalf("recommendation %d samples = %d, want %d", index, recommendation.Samples, expectedSamples[key])
		}
	}
}

func TestSQLDataflowIndexAdvisorSupportsConcurrentObservationAndRecommendations(t *testing.T) {
	advisor := NewSQLDataflowIndexAdvisor(SQLDataflowIndexAdvisorOptions{
		Capacity:   1,
		MinSamples: 1,
		CostModel:  SQLArrangementCostModelOptions{MemoryBudgetBytes: 1024},
	})
	observation := SQLDataflowIndexObservation{
		Candidate: SQLArrangementCostCandidate{
			Key:            "users",
			Field:          "region",
			ProbeCostNanos: 10,
			ScanCostNanos:  100,
			ExpectedReads:  10,
			MemoryBytes:    512,
		},
	}
	var group sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for iteration := 0; iteration < 64; iteration++ {
				advisor.ObserveDataflow(observation)
				advisor.Recommendations(1)
			}
		}()
	}
	group.Wait()
	if recommendations := advisor.Recommendations(1); len(recommendations) != 1 {
		t.Fatalf("expected one concurrent recommendation, got %d", len(recommendations))
	}
}

func ExampleSQLDataflowIndexAdvisor() {
	advisor := NewSQLDataflowIndexAdvisor(SQLDataflowIndexAdvisorOptions{
		Capacity:   16,
		MinSamples: 2,
		CostModel:  SQLArrangementCostModelOptions{MemoryBudgetBytes: 1024},
	})
	observation := SQLDataflowIndexObservation{
		Candidate: SQLArrangementCostCandidate{
			Key:              "users",
			Field:            "region",
			BuildCostNanos:   100,
			ProbeCostNanos:   10,
			ScanCostNanos:    110,
			MaintenanceNanos: 5,
			ExpectedReads:    10,
			ExpectedWrites:   1,
			MemoryBytes:      512,
		},
	}
	advisor.ObserveDataflows([]SQLDataflowIndexObservation{observation, observation})
	recommendations := advisor.Recommendations(1)
	fmt.Printf("%s.%s samples=%d reusable=%t payback=%d\n",
		recommendations[0].Candidate.Key,
		recommendations[0].Candidate.Field,
		recommendations[0].Samples,
		recommendations[0].Reusable,
		recommendations[0].PaybackReads,
	)
	// Output:
	// users.region samples=2 reusable=true payback=2
}
