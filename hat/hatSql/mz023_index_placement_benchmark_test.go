package hatSql

import "testing"

func BenchmarkMZ023LegacyIndexedResolution(b *testing.B) {
	resolver := &mz023LegacyIndexedResolver{rows: []Row{{"id": int64(42), "name": "Ada"}}}
	source := sqlSource{kind: "CACHE", key: "people", alias: "p"}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		rows, available, err := resolveSQLIndexedComparison(source, "id", "=", int64(42), resolver, "")
		if err != nil || !available || len(rows) != 1 {
			b.Fatalf("legacy lookup failed: rows=%d available=%v err=%v", len(rows), available, err)
		}
	}
}

func BenchmarkMZ023ClusterIndexedResolution(b *testing.B) {
	resolver := &mz023ClusterIndexedResolver{rows: []Row{{"id": int64(42), "name": "Ada"}}}
	source := sqlSource{kind: "CACHE", key: "people", alias: "p"}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		rows, available, err := resolveSQLIndexedComparison(source, "id", "=", int64(42), resolver, "analytics")
		if err != nil || !available || len(rows) != 1 {
			b.Fatalf("cluster lookup failed: rows=%d available=%v err=%v", len(rows), available, err)
		}
	}
}

func BenchmarkMZ023IndexStrategyGlobalPlacement(b *testing.B) {
	benchmarkMZ023IndexStrategy(b, SQLIndexHint{Field: "id", Mode: SQLIndexHintForce})
}

func BenchmarkMZ023IndexStrategyClusterPlacement(b *testing.B) {
	benchmarkMZ023IndexStrategy(b, SQLIndexHint{Field: "id", Cluster: "analytics", Mode: SQLIndexHintForce})
}

func benchmarkMZ023IndexStrategy(b *testing.B, hint SQLIndexHint) {
	candidates := make([]SQLIndexStrategyCandidate, 32)
	cluster := hint.Cluster
	for index := range candidates {
		candidates[index] = SQLIndexStrategyCandidate{
			SQLIndexDefinition: SQLIndexDefinition{Key: "people_by_id", Field: "id", Kind: "HASH", Cluster: cluster},
			Priority:           index,
			EstimatedRows:      index + 1,
			Available:          true,
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		decision, err := ExplainSQLIndexStrategy("people", "id", hint, candidates)
		if err != nil || !decision.HasSelection {
			b.Fatalf("strategy selection failed: decision=%+v err=%v", decision, err)
		}
	}
}
