//go:build !mz045baseline

package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestMZ045ArrangementRecommendationCachePublicAPI(t *testing.T) {
	cache, err := hatSql.NewSQLArrangementRecommendationCache(hatSql.SQLArrangementRecommendationCacheOptions{MaxEntries: 2})
	if err != nil {
		t.Fatal(err)
	}
	recommendation := cache.Recommend(
		"CACHE",
		"events",
		"schema-v1",
		[]hatSql.SQLArrangementMetadata{{Key: "tenant_id", Kind: "hash-index", Fields: []string{"tenant_id"}}},
		hatSql.SQLArrangementWorkload{FilterFields: []string{"tenant_id"}},
	)
	if recommendation.Key != "tenant_id" {
		t.Fatalf("public recommendation = %#v, want tenant_id", recommendation)
	}
}
