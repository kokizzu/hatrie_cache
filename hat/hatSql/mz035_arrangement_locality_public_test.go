package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestMZ035ArrangementLocalityPublicAPI(t *testing.T) {
	workload := hatSql.SQLArrangementWorkload{LocalityHints: []string{"zone-a"}}
	candidates := []hatSql.SQLArrangementMetadata{{Key: "id", Locality: "zone-a"}}
	if recommendation := hatSql.RecommendSQLArrangement(candidates, workload); recommendation.Key != "" {
		t.Fatalf("locality-only recommendation = %#v, want no field-driven choice", recommendation)
	}
}
