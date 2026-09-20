package hatSql

import "testing"

var (
	tu26BenchmarkDecision  SQLIndexStrategyDecision
	tu26BenchmarkKind       string
	tu26BenchmarkCandidates = []SQLIndexStrategyCandidate{
		{
			SQLIndexDefinition: SQLIndexDefinition{Key: "people_by_id_hash", Field: "id", Kind: "HASH"},
			Priority:            10,
			EstimatedRows:       1,
			EstimatedCost:       1,
			Available:            true,
		},
		{
			SQLIndexDefinition: SQLIndexDefinition{Key: "people_by_id_ordered", Field: "id", Kind: "ORDERED"},
			Priority:            20,
			EstimatedRows:       1,
			EstimatedCost:       2,
			Available:            true,
		},
		{
			SQLIndexDefinition: SQLIndexDefinition{Key: "people_by_name", Field: "name", Kind: "HASH"},
			Priority:            1,
			EstimatedRows:       50,
			EstimatedCost:       4,
			Available:            true,
		},
		{
			SQLIndexDefinition: SQLIndexDefinition{Key: "people_by_id_old", Field: "id", Kind: "HASH"},
			Priority:            30,
			EstimatedRows:       100,
			EstimatedCost:       100,
			Available:            false,
		},
	}
)

func tu26LegacyStrategySelection(candidates []SQLIndexStrategyCandidate) (string, bool) {
	selected := -1
	for index, candidate := range candidates {
		if !candidate.Available || candidate.Field != "id" {
			continue
		}
		if selected < 0 || candidate.Priority < candidates[selected].Priority ||
			(candidate.Priority == candidates[selected].Priority && candidate.EstimatedCost < candidates[selected].EstimatedCost) {
			selected = index
		}
	}
	if selected < 0 {
		return "", false
	}
	return candidates[selected].Kind, true
}

func BenchmarkTU26LegacyStrategySelection(b *testing.B) {
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		tu26BenchmarkKind, _ = tu26LegacyStrategySelection(tu26BenchmarkCandidates)
	}
}

func BenchmarkTU26ExplainSQLIndexStrategy(b *testing.B) {
	b.ReportAllocs()
	hint := SQLIndexHint{Field: "id", Mode: SQLIndexHintForce}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		decision, err := ExplainSQLIndexStrategy("people", "id", hint, tu26BenchmarkCandidates)
		if err != nil {
			b.Fatal(err)
		}
		tu26BenchmarkDecision = decision
	}
}
