package hatSql

import "testing"

func BenchmarkM218BaselineAlwaysScanLookup(b *testing.B) {
	rows := m217BenchmarkRows(10_000)
	b.ReportAllocs()
	b.ResetTimer()
	var result []Row
	for b.Loop() {
		result = m217ScanPointLookup(rows, "team-42")
	}
	b.StopTimer()
	b.ReportMetric(float64(len(result)), "result_rows")
}

func BenchmarkM218CostBasedPointLookup(b *testing.B) {
	rows := m217BenchmarkRows(10_000)
	index, err := NewIncrementalPointLookup(IncrementalPointLookupDefinition{
		IndexKey: func(row Row) (string, error) {
			return row["team"].(string), nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	updates := make([]DifferentialRow, len(rows))
	for index, row := range rows {
		updates[index] = DifferentialRow{Key: row["id"].(string), Time: 1, Diff: 1, Row: row}
	}
	if err := index.Apply(updates); err != nil {
		b.Fatal(err)
	}
	candidates := []SQLPointLookupCandidate{{
		SQLArrangementCostCandidate: SQLArrangementCostCandidate{
			Key:              "team",
			Field:            "team",
			Kind:             "point-lookup",
			BuildCostNanos:   100,
			ProbeCostNanos:   100_000,
			ScanCostNanos:    300_000,
			MaintenanceNanos: 100,
			ExpectedReads:    100,
			ExpectedWrites:   1,
			MemoryBytes:      8 << 20,
		},
		Available: true,
	}}
	b.ReportAllocs()
	b.ResetTimer()
	var result []DifferentialRow
	for b.Loop() {
		plan := PlanSQLPointLookup(candidates, SQLPointLookupPlanOptions{MemoryBudgetBytes: 16 << 20})
		if plan.HasSelection {
			result = index.Lookup("team-42")
			continue
		}
		fallback := m217ScanPointLookup(rows, "team-42")
		result = make([]DifferentialRow, len(fallback))
		for resultIndex, row := range fallback {
			result[resultIndex] = DifferentialRow{Key: row["id"].(string), Diff: 1, Row: row}
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(len(result)), "result_rows")
}

func BenchmarkM218PlannerDecision32Candidates(b *testing.B) {
	candidates := make([]SQLPointLookupCandidate, 32)
	for index := range candidates {
		candidates[index] = SQLPointLookupCandidate{
			SQLArrangementCostCandidate: SQLArrangementCostCandidate{
				Key:            "candidate-" + string(rune('a'+index)),
				Field:          "team",
				Kind:           "point-lookup",
				ProbeCostNanos: 100 + uint64(index),
				ScanCostNanos:  300_000,
				ExpectedReads:  100,
				MemoryBytes:    uint64(index+1) * 1024,
			},
			Available: true,
		}
	}
	b.ReportAllocs()
	for b.Loop() {
		_ = PlanSQLPointLookup(candidates, SQLPointLookupPlanOptions{MemoryBudgetBytes: 1 << 20})
	}
}
