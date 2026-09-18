package hatSql

import (
	"strings"
	"testing"
)

func TestMZ035ArrangementLocalityHintPrefersMatchingCandidate(t *testing.T) {
	candidates := []SQLArrangementMetadata{
		{Key: "user_id:west", Kind: "hash index", Fields: []string{"user_id"}, Locality: "us-west"},
		{Key: "user_id:east", Kind: "hash index", Fields: []string{"user_id"}, Locality: "us-east"},
	}
	recommendation := RecommendSQLArrangement(candidates, SQLArrangementWorkload{
		FilterFields:  []string{"user_id"},
		LocalityHints: []string{" US-EAST "},
	})
	if recommendation.Key != "user_id:east" {
		t.Fatalf("locality recommendation = %#v, want east candidate", recommendation)
	}
	if !strings.Contains(recommendation.Reason, "LOCALITY") {
		t.Fatalf("locality recommendation reason = %q, want LOCALITY", recommendation.Reason)
	}
}

func TestMZ035NoLocalityHintPreservesExistingChoice(t *testing.T) {
	candidates := []SQLArrangementMetadata{
		{Key: "user_id:z", Kind: "hash index", Fields: []string{"user_id"}, Locality: "us-west"},
		{Key: "user_id:a", Kind: "hash index", Fields: []string{"user_id"}, Locality: "us-east"},
	}
	recommendation := RecommendSQLArrangement(candidates, SQLArrangementWorkload{FilterFields: []string{"user_id"}})
	if recommendation.Key != "user_id:a" {
		t.Fatalf("no-hint recommendation = %#v, want deterministic lexical choice", recommendation)
	}
}

func TestMZ035LocalityHintsAreBoundedAndNormalized(t *testing.T) {
	hints := make([]string, maxSQLArrangementSelectorFields+4)
	for index := range hints {
		hints[index] = "ignored"
	}
	hints[0] = "  eu-central  "
	candidates := []SQLArrangementMetadata{{Key: "value:eu", Fields: []string{"value"}, Locality: "EU-CENTRAL"}}
	recommendation := RecommendSQLArrangement(candidates, SQLArrangementWorkload{FilterFields: []string{"value"}, LocalityHints: hints})
	if recommendation.Key != "value:eu" {
		t.Fatalf("bounded locality recommendation = %#v, want matching candidate", recommendation)
	}
}
