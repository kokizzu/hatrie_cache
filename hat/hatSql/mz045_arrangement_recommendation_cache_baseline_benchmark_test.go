//go:build mz045baseline

package hatSql

import "testing"

var mz045BaselineArrangementRecommendationSink SQLArrangementRecommendation

func BenchmarkMZ045ArrangementRecommendationBaseline(b *testing.B) {
	candidates := []SQLArrangementMetadata{
		{Key: "tenant_id", Kind: "hash-index", Fields: []string{"tenant_id"}, MemoryBytes: 1024},
		{Key: "region", Kind: "sorted-aggregate", Fields: []string{"region"}, MemoryBytes: 2048},
		{Key: "created_at", Kind: "sorted", Fields: []string{"created_at"}, MemoryBytes: 4096},
	}
	workload := SQLArrangementWorkload{FilterFields: []string{"tenant_id"}, GroupByFields: []string{"region"}}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		mz045BaselineArrangementRecommendationSink = RecommendSQLArrangement(candidates, workload)
	}
}
