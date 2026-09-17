package hatDataStructure

import (
	"errors"
	"reflect"
	"sync"
	"testing"
)

func TestCHU48RankedTokenPostingsIndex(t *testing.T) {
	index := NewRankedTokenPostingsIndex()
	index.Upsert(1, "go go fast")
	index.Upsert(2, "go")
	index.Upsert(3, "fast")
	index.Upsert(4, "unrelated")

	got, err := index.MatchRanked("go fast", TokenPostingsRankOptions{Limit: 3})
	if err != nil {
		t.Fatalf("MatchRanked returned error: %v", err)
	}
	if rows := rankedTokenPostingRows(got); !reflect.DeepEqual(rows, []uint32{1, 2, 3}) {
		t.Fatalf("ranked rows = %v, want [1 2 3]", rows)
	}
	if got[0].Score <= got[1].Score || got[1].Score != got[2].Score {
		t.Fatalf("ranked scores = %v, want row 1 greater and rows 2/3 tied", got)
	}

	limited, err := index.MatchRanked("go fast", TokenPostingsRankOptions{Limit: 2})
	if err != nil {
		t.Fatalf("limited MatchRanked returned error: %v", err)
	}
	if rows := rankedTokenPostingRows(limited); !reflect.DeepEqual(rows, []uint32{1, 2}) {
		t.Fatalf("limited ranked rows = %v, want [1 2]", rows)
	}

	dst := []TokenPostingsRankedRow{{Row: 99, Score: 7}}
	appended, err := index.MatchRankedInto(dst, "go fast", TokenPostingsRankOptions{Limit: 1})
	if err != nil {
		t.Fatalf("MatchRankedInto returned error: %v", err)
	}
	wantAppended := []TokenPostingsRankedRow{{Row: 99, Score: 7}, got[0]}
	if !reflect.DeepEqual(appended, wantAppended) {
		t.Fatalf("MatchRankedInto = %v, want %v", appended, wantAppended)
	}

	before := got[0].Score
	index.Upsert(1, "go")
	afterResults, err := index.MatchRanked("go fast", TokenPostingsRankOptions{Limit: 3})
	if err != nil {
		t.Fatalf("MatchRanked after update returned error: %v", err)
	}
	if after := rankedTokenPostingScore(afterResults, 1); after >= before {
		t.Fatalf("row 1 score after removing repeated term = %v, want less than %v", after, before)
	}
	if !index.Delete(1) {
		t.Fatal("Delete(1) = false, want true")
	}
	if afterDelete, err := index.MatchRanked("go fast", TokenPostingsRankOptions{Limit: 3}); err != nil {
		t.Fatalf("MatchRanked after delete returned error: %v", err)
	} else if rankedTokenPostingScore(afterDelete, 1) != 0 {
		t.Fatalf("row 1 remained after delete: %v", afterDelete)
	}
}

func TestCHU48RankedTokenPostingsIndexRequiresOptInAndValidOptions(t *testing.T) {
	plain := NewTokenPostingsIndex()
	plain.Upsert(1, "go")
	if _, err := plain.MatchRanked("go", TokenPostingsRankOptions{}); !errors.Is(err, ErrTokenPostingsRankingDisabled) {
		t.Fatalf("plain MatchRanked error = %v, want ErrTokenPostingsRankingDisabled", err)
	}

	ranked := NewRankedTokenPostingsIndex()
	ranked.Upsert(1, "go")
	if _, err := ranked.MatchRanked("go", TokenPostingsRankOptions{Limit: -1}); !errors.Is(err, ErrTokenPostingsInvalidRankOptions) {
		t.Fatalf("negative limit error = %v, want ErrTokenPostingsInvalidRankOptions", err)
	}
	if _, err := ranked.MatchRanked("go", TokenPostingsRankOptions{K1: -1}); !errors.Is(err, ErrTokenPostingsInvalidRankOptions) {
		t.Fatalf("negative K1 error = %v, want ErrTokenPostingsInvalidRankOptions", err)
	}
	if _, err := ranked.MatchRanked("go", TokenPostingsRankOptions{B: 2}); !errors.Is(err, ErrTokenPostingsInvalidRankOptions) {
		t.Fatalf("out-of-range B error = %v, want ErrTokenPostingsInvalidRankOptions", err)
	}

	dst := []TokenPostingsRankedRow{{Row: 8, Score: 1}}
	got, err := plain.MatchRankedInto(dst, "!!!", TokenPostingsRankOptions{})
	if err != nil {
		t.Fatalf("tokenless MatchRankedInto returned error: %v", err)
	}
	if !reflect.DeepEqual(got, dst) {
		t.Fatalf("tokenless MatchRankedInto = %v, want unchanged %v", got, dst)
	}
}

func TestCHU48RankedTokenPostingsIndexLifecycleAndConcurrentQueries(t *testing.T) {
	index := NewRankedTokenPostingsIndex()
	index.Upsert(1, "single")
	index.Upsert(1, "single extra")
	if got := index.RowsForToken("single"); !reflect.DeepEqual(got, []uint32{1}) {
		t.Fatalf("retained term rows = %v, want [1]", got)
	}
	index.Upsert(2, "shared shared")

	var waitGroup sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		waitGroup.Add(1)
		go func(worker int) {
			defer waitGroup.Done()
			for iteration := 0; iteration < 50; iteration++ {
				row := uint32(worker*1000 + iteration + 10)
				index.Upsert(row, "shared ranked")
				if _, err := index.MatchRanked("shared ranked", TokenPostingsRankOptions{Limit: 5}); err != nil {
					t.Errorf("concurrent MatchRanked returned error: %v", err)
				}
				if iteration%2 == 0 {
					index.Delete(row)
				}
			}
		}(worker)
	}
	waitGroup.Wait()

	index.Clear()
	if index.Len() != 0 || index.TermCount() != 0 {
		t.Fatalf("after Clear Len/TermCount = %d/%d, want 0/0", index.Len(), index.TermCount())
	}
	index.Upsert(7, "after clear")
	rows, err := index.MatchRanked("after", TokenPostingsRankOptions{})
	if err != nil {
		t.Fatalf("MatchRanked after Clear returned error: %v", err)
	}
	if !reflect.DeepEqual(rankedTokenPostingRows(rows), []uint32{7}) {
		t.Fatalf("rows after Clear/reuse = %v, want [7]", rankedTokenPostingRows(rows))
	}
}

func rankedTokenPostingRows(rows []TokenPostingsRankedRow) []uint32 {
	result := make([]uint32, len(rows))
	for index, row := range rows {
		result[index] = row.Row
	}
	return result
}

func rankedTokenPostingScore(rows []TokenPostingsRankedRow, rowID uint32) float64 {
	for _, row := range rows {
		if row.Row == rowID {
			return row.Score
		}
	}
	return 0
}
