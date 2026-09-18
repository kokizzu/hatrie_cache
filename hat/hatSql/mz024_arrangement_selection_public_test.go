package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestMZ024ArrangementSelectionIsImportable(t *testing.T) {
	recommendation := hatSql.RecommendSQLArrangement(
		[]hatSql.SQLArrangementMetadata{{
			Key:    "region",
			Kind:   "sorted-aggregate",
			Fields: []string{"region"},
		}},
		hatSql.SQLArrangementWorkload{GroupByFields: []string{"region"}},
	)
	if recommendation.Key != "region" {
		t.Fatalf("recommendation key = %q, want region", recommendation.Key)
	}
}
