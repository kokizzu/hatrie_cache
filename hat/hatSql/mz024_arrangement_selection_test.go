package hatSql

import "testing"

var mz024ArrangementRecommendationSink SQLArrangementRecommendation

func TestMZ024ArrangementSelectionPrefersQueryShape(t *testing.T) {
	workload := SQLArrangementWorkload{
		FilterFields:  []string{"tenant_id"},
		GroupByFields: []string{"region"},
		OrderByFields: []string{"created_at"},
	}
	candidates := []SQLArrangementMetadata{
		{
			Key:         "tenant_id",
			Kind:        "hash-index",
			Reused:      true,
			Fields:      []string{"tenant_id"},
			Cardinality: 100,
			MemoryBytes: 1024,
		},
		{
			Key:         "region,created_at",
			Kind:        "sorted-aggregate",
			Reused:      true,
			Fields:      []string{"region", "created_at"},
			Cardinality: 200,
			MemoryBytes: 2048,
		},
	}

	recommendation := RecommendSQLArrangement(candidates, workload)
	if recommendation.Key != "region,created_at" {
		t.Fatalf("recommended arrangement = %q, want region,created_at", recommendation.Key)
	}
	if recommendation.Score <= 0 {
		t.Fatalf("recommendation score = %d, want positive", recommendation.Score)
	}
}

func TestMZ024ArrangementSelectionRejectsUnrelatedCandidates(t *testing.T) {
	recommendation := RecommendSQLArrangement(
		[]SQLArrangementMetadata{{Key: "customer_id", Kind: "hash-index", Fields: []string{"customer_id"}}},
		SQLArrangementWorkload{OrderByFields: []string{"created_at"}},
	)
	if recommendation.Key != "" || recommendation.Score != 0 {
		t.Fatalf("unrelated recommendation = %#v, want empty", recommendation)
	}
}

func TestMZ024ArrangementSelectionMarksExplainMetadata(t *testing.T) {
	arrangements := []SQLArrangementMetadata{
		{Key: "tenant_id", Kind: "hash-index", Fields: []string{"tenant_id"}},
		{Key: "region", Kind: "sorted-aggregate", Fields: []string{"region"}},
	}
	sqlMarkArrangementRecommendation(arrangements, SQLArrangementWorkload{GroupByFields: []string{"region"}})
	if arrangements[0].Recommended {
		t.Fatal("unrelated arrangement was marked recommended")
	}
	if !arrangements[1].Recommended || arrangements[1].MatchScore <= 0 || arrangements[1].Recommendation == "" {
		t.Fatalf("arrangements = %#v, want one scored recommendation", arrangements)
	}
}

type mz024ExplainResolver struct{}

func (mz024ExplainResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return []Row{{"region": "apac"}}, nil
}

func (mz024ExplainResolver) ResolveSQLArrangementMetadata(string, string) ([]SQLArrangementMetadata, error) {
	return []SQLArrangementMetadata{
		{Key: "region", Kind: "sorted-aggregate", Fields: []string{"region"}},
		{Key: "customer_id", Kind: "hash-index", Fields: []string{"customer_id"}},
	}, nil
}

func TestMZ024ExplainMarksBestArrangement(t *testing.T) {
	query := &sqlQuery{
		from:    &sqlSource{kind: "CACHE", key: "events"},
		selects: []sqlSelectItem{{expr: sqlExpr{kind: "field", name: "region"}}},
		groupBy: []sqlExpr{{kind: "field", name: "region"}},
	}
	steps := sqlExplainStepsWithResolver(query, mz024ExplainResolver{})
	if len(steps) == 0 || len(steps[0].Arrangements) != 2 {
		t.Fatalf("explain steps = %#v, want scan arrangements", steps)
	}
	if !steps[0].Arrangements[0].Recommended || steps[0].Arrangements[0].Key != "region" {
		t.Fatalf("scan arrangements = %#v, want region recommendation", steps[0].Arrangements)
	}
	if steps[0].Arrangements[1].Recommended {
		t.Fatal("unrelated arrangement was marked recommended in EXPLAIN")
	}
}

func BenchmarkMZ024ArrangementSelection(b *testing.B) {
	candidates := make([]SQLArrangementMetadata, 32)
	for index := range candidates {
		candidates[index] = SQLArrangementMetadata{
			Key:         "field_" + string(rune('a'+index)),
			Kind:        "sorted",
			Fields:      []string{"field_" + string(rune('a'+index))},
			Cardinality: index + 1,
			MemoryBytes: (index + 1) * 1024,
		}
	}
	candidates[17] = SQLArrangementMetadata{
		Key:         "region,created_at",
		Kind:        "sorted-aggregate",
		Reused:      true,
		Fields:      []string{"region", "created_at"},
		Cardinality: 512,
		MemoryBytes: 32 * 1024,
	}
	workload := SQLArrangementWorkload{
		FilterFields:  []string{"tenant_id"},
		GroupByFields: []string{"region"},
		OrderByFields: []string{"created_at"},
	}
	b.ReportAllocs()
	for b.Loop() {
		mz024ArrangementRecommendationSink = RecommendSQLArrangement(candidates, workload)
	}
}
