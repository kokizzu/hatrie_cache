package hatDataStructure

import (
	"reflect"
	"strconv"
	"testing"
)

func TestCHU13PhrasePostingsIndexLifecycleAndVisit(t *testing.T) {
	index := NewTokenPostingsIndexWithPhrases()
	index.Upsert(1, "alpha beta gamma")
	index.Upsert(2, "alpha gamma beta")
	index.Upsert(3, "prefix alpha beta gamma suffix")

	if got := index.MatchPhrase("alpha beta gamma"); !reflect.DeepEqual(got, []uint32{1, 3}) {
		t.Fatalf("initial phrase rows = %v, want [1 3]", got)
	}

	dst := index.MatchPhraseInto([]uint32{99}, "alpha beta gamma")
	if !reflect.DeepEqual(dst, []uint32{99, 1, 3}) {
		t.Fatalf("phrase rows with destination = %v, want [99 1 3]", dst)
	}

	var visited []uint32
	if !index.VisitPhrase("alpha beta gamma", func(row uint32) bool {
		visited = append(visited, row)
		return true
	}) {
		t.Fatal("VisitPhrase returned false, want true")
	}
	if !reflect.DeepEqual(visited, []uint32{1, 3}) {
		t.Fatalf("visited phrase rows = %v, want [1 3]", visited)
	}

	index.Upsert(1, "alpha beta beta gamma")
	if got := index.MatchPhrase("alpha beta gamma"); !reflect.DeepEqual(got, []uint32{3}) {
		t.Fatalf("phrase rows after order/repetition update = %v, want [3]", got)
	}
	if got := index.MatchPhrase("alpha beta beta"); !reflect.DeepEqual(got, []uint32{1}) {
		t.Fatalf("repeated-token phrase rows = %v, want [1]", got)
	}

	if !index.Delete(3) {
		t.Fatal("Delete(3) = false, want true")
	}
	index.Upsert(3, "new value")
	if got := index.MatchPhrase("alpha beta gamma"); len(got) != 0 {
		t.Fatalf("deleted phrase rows after term reuse = %v, want empty", got)
	}

	info := index.PhraseInfo()
	if info.Rows != 3 || info.Tokens != 9 || info.SequenceBytes != 9 {
		t.Fatalf("phrase info = %+v, want rows=3 tokens=9 sequence_bytes=9", info)
	}

	index.Clear()
	if info := index.PhraseInfo(); info != (TokenPhrasePostingsIndexInfo{}) {
		t.Fatalf("phrase info after Clear = %+v, want zero", info)
	}
	index.Upsert(4, "after clear works")
	if got := index.MatchPhrase("after clear"); !reflect.DeepEqual(got, []uint32{4}) {
		t.Fatalf("phrase rows after Clear = %v, want [4]", got)
	}
}

func TestCHU13PhrasePostingsIndexVarintTermIDs(t *testing.T) {
	index := NewTokenPostingsIndexWithPhrases()
	for row := uint32(10); row < 210; row++ {
		index.Upsert(row, "filler"+strconv.FormatUint(uint64(row), 10))
	}
	index.Upsert(1, "needle marker")

	if got := index.MatchPhrase("needle marker"); !reflect.DeepEqual(got, []uint32{1}) {
		t.Fatalf("high-term-ID phrase rows = %v, want [1]", got)
	}
	info := index.PhraseInfo()
	if info.Rows != 201 || info.Tokens != 202 {
		t.Fatalf("high-term-ID phrase info = %+v, want rows=201 tokens=202", info)
	}
}
