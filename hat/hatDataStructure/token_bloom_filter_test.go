package hatDataStructure_test

import (
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestTokenBloomFilterMatchesNormalizedWordsAndRoundTrips(t *testing.T) {
	filter, err := hatDataStructure.NewTokenBloomFilterWithShape(1<<20, 3)
	if err != nil {
		t.Fatalf("NewTokenBloomFilterWithShape() error = %v", err)
	}
	if filter.AddText("Fast, low-memory storage for café users") == false {
		t.Fatal("AddText() did not report changed bits")
	}
	for _, token := range []string{"fast", "LOW", "MEMORY", "CAFÉ", "users"} {
		if !filter.ContainsToken(token) {
			t.Fatalf("ContainsToken(%q) = false for inserted token", token)
		}
	}
	if !filter.ContainsAllTokens("FAST storage users") {
		t.Fatal("ContainsAllTokens() rejected inserted words")
	}
	if !filter.ContainsAllTokens("storage, café") {
		t.Fatal("ContainsAllTokens() rejected punctuation-separated words")
	}
	if !filter.ContainsAnyTokens("missing, users") {
		t.Fatal("ContainsAnyTokens() rejected an inserted word")
	}
	if !filter.AddToken("ПРИВЕТ") || !filter.ContainsToken("привет") {
		t.Fatal("AddToken() did not apply Unicode lower-casing")
	}
	if !filter.ContainsAllTokens("   ") {
		t.Fatal("ContainsAllTokens() rejected an empty token query")
	}

	snapshot := filter.Snapshot()
	restored, err := hatDataStructure.NewTokenBloomFilterFromSnapshot(snapshot)
	if err != nil {
		t.Fatalf("NewTokenBloomFilterFromSnapshot() error = %v", err)
	}
	if restored.Info() != filter.Info() {
		t.Fatalf("snapshot changed info: restored=%+v original=%+v", restored.Info(), filter.Info())
	}
	if !restored.ContainsAllTokens("fast café users") {
		t.Fatal("snapshot round trip lost inserted words")
	}
}

func TestTokenBloomFilterZeroValueAndEmptyTokenBehavior(t *testing.T) {
	var filter hatDataStructure.TokenBloomFilter
	if filter.AddText("ignored") {
		t.Fatal("zero-value AddText() reported a change")
	}
	if filter.ContainsToken("") {
		t.Fatal("empty token reported as present")
	}
	if !filter.ContainsAllTokens("") {
		t.Fatal("empty token query should impose no filter")
	}
	if filter.ContainsAnyTokens("") {
		t.Fatal("empty token query should not match any token")
	}
	if filter.AddToken("not-a-token") {
		t.Fatal("AddToken() accepted punctuation-containing input")
	}
}
