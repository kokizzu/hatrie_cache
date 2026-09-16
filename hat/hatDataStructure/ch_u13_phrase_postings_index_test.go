package hatDataStructure

import (
	"reflect"
	"testing"
)

func TestCHU13PhrasePostingsIndexMatchesContiguousNormalizedTokens(t *testing.T) {
	index := NewTokenPostingsIndexWithPhrases()
	index.Upsert(1, "Quick, brown fox")
	index.Upsert(2, "quick fox brown")
	index.Upsert(3, "The quick brown fox")
	index.Upsert(4, "quick brown")
	index.Upsert(5, "quick brown brown fox")

	if got := index.MatchPhrase("QUICK brown fox"); !reflect.DeepEqual(got, []uint32{1, 3}) {
		t.Fatalf("phrase rows = %v, want [1 3]", got)
	}
	if got := index.MatchPhrase("brown fox"); !reflect.DeepEqual(got, []uint32{1, 3, 5}) {
		t.Fatalf("short phrase rows = %v, want [1 3 5]", got)
	}
	if got := index.MatchPhrase("quick brown"); !reflect.DeepEqual(got, []uint32{1, 3, 4, 5}) {
		t.Fatalf("prefix phrase rows = %v, want [1 3 4 5]", got)
	}
	if got := index.MatchPhrase("quick brown fox"); !reflect.DeepEqual(got, []uint32{1, 3}) {
		t.Fatalf("phrase with repeated middle token rows = %v, want [1 3]", got)
	}

	dst := []uint32{99}
	if got := index.MatchPhraseInto(dst, "missing phrase"); !reflect.DeepEqual(got, dst) {
		t.Fatalf("missing phrase destination = %v, want unchanged %v", got, dst)
	}

	index.Upsert(2, "quick brown fox")
	if got := index.MatchPhrase("quick brown fox"); !reflect.DeepEqual(got, []uint32{1, 2, 3}) {
		t.Fatalf("phrase rows after update = %v, want [1 2 3]", got)
	}
	if got := index.MatchPhrase("quick fox"); len(got) != 0 {
		t.Fatalf("old phrase rows after update = %v, want empty", got)
	}
}

func TestCHU13PhrasePostingsIndexIsOptIn(t *testing.T) {
	var index TokenPostingsIndex
	index.Upsert(1, "quick brown fox")
	if got := index.MatchPhrase("quick brown fox"); got != nil {
		t.Fatalf("default index phrase rows = %v, want nil without phrase storage", got)
	}
}
