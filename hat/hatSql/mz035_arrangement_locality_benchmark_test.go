package hatSql

import "testing"

var mz035ArrangementRecommendationSink SQLArrangementRecommendation

func BenchmarkMZ035ArrangementRecommendationWithoutLocality(b *testing.B) {
	candidates := []SQLArrangementMetadata{
		{Key: "user_id:west", Kind: "hash index", Fields: []string{"user_id"}, Locality: "us-west"},
		{Key: "user_id:east", Kind: "hash index", Fields: []string{"user_id"}, Locality: "us-east"},
	}
	workload := SQLArrangementWorkload{FilterFields: []string{"user_id"}}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		mz035ArrangementRecommendationSink = RecommendSQLArrangement(candidates, workload)
	}
}

func BenchmarkMZ035ArrangementRecommendationWithLocality(b *testing.B) {
	candidates := []SQLArrangementMetadata{
		{Key: "user_id:west", Kind: "hash index", Fields: []string{"user_id"}, Locality: "us-west"},
		{Key: "user_id:east", Kind: "hash index", Fields: []string{"user_id"}, Locality: "us-east"},
	}
	workload := SQLArrangementWorkload{FilterFields: []string{"user_id"}, LocalityHints: []string{"us-east"}}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		mz035ArrangementRecommendationSink = RecommendSQLArrangement(candidates, workload)
	}
}
