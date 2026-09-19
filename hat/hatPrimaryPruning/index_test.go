package hatPrimaryPruning

import (
	"errors"
	"reflect"
	"sync"
	"testing"
)

func TestIndexPrunesCompositeRanges(t *testing.T) {
	index, err := Build([]Mark{
		{Min: []KeyPart{{Value: 1}, {Value: 0}}, Max: []KeyPart{{Value: 1}, {Value: 99}}},
		{Min: []KeyPart{{Value: 2}, {Value: 0}}, Max: []KeyPart{{Value: 2}, {Value: 99}}},
		{Min: []KeyPart{{Value: 3}, {Value: 0}}, Max: []KeyPart{{Value: 3}, {Value: 99}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	query := Range{
		HasLower: true,
		Lower:    []KeyPart{{Value: 2}, {Value: 50}},
		HasUpper: true,
		Upper:    []KeyPart{{Value: 2}, {Value: 60}},
	}
	got, err := index.Candidates(query, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []int{1}) {
		t.Fatalf("Candidates = %v, want [1]", got)
	}
	boundary := Range{
		HasLower: true,
		Lower:    []KeyPart{{Value: 1}, {Value: 99}},
		HasUpper: true,
		Upper:    []KeyPart{{Value: 2}, {Value: 0}},
	}
	got, err = index.Candidates(boundary, got[:0])
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []int{0, 1}) {
		t.Fatalf("boundary Candidates = %v, want [0 1]", got)
	}
}

func TestIndexHonorsNullOrdering(t *testing.T) {
	index, err := BuildWithConfig([]Mark{
		{Min: []KeyPart{{Value: 1}}, Max: []KeyPart{{Null: true}}},
		{Min: []KeyPart{{Value: 2}}, Max: []KeyPart{{Value: 3}}},
	}, Config{NullsLast: true})
	if err != nil {
		t.Fatal(err)
	}
	got, err := index.Candidates(Range{HasLower: true, Lower: []KeyPart{{Null: true}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []int{0}) {
		t.Fatalf("NULLS LAST candidates = %v, want [0]", got)
	}

	index, err = BuildWithConfig([]Mark{
		{Min: []KeyPart{{Null: true}}, Max: []KeyPart{{Value: 1}}},
		{Min: []KeyPart{{Value: 2}}, Max: []KeyPart{{Value: 3}}},
	}, Config{NullsLast: false})
	if err != nil {
		t.Fatal(err)
	}
	got, err = index.Candidates(Range{HasUpper: true, Upper: []KeyPart{{Null: true}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []int{0}) {
		t.Fatalf("NULLS FIRST candidates = %v, want [0]", got)
	}
}

func TestIndexValidatesMarksAndRanges(t *testing.T) {
	if _, err := Build([]Mark{{Min: []KeyPart{{Value: 2}}, Max: []KeyPart{{Value: 1}}}}); !errors.Is(err, ErrInvalidMark) {
		t.Fatalf("invalid mark error = %v, want %v", err, ErrInvalidMark)
	}
	index, err := Build([]Mark{{Min: []KeyPart{{Value: 1}}, Max: []KeyPart{{Value: 2}}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := index.Candidates(Range{HasLower: true, Lower: []KeyPart{{Value: 3}}, HasUpper: true, Upper: []KeyPart{{Value: 2}}}, nil); !errors.Is(err, ErrInvalidRange) {
		t.Fatalf("invalid range error = %v, want %v", err, ErrInvalidRange)
	}
	if _, err := index.Candidates(Range{HasLower: true, Lower: []KeyPart{{Value: 1}, {Value: 2}}}, nil); !errors.Is(err, ErrArityMismatch) {
		t.Fatalf("arity error = %v, want %v", err, ErrArityMismatch)
	}
}

func TestIndexCopiesMarksAndSupportsConcurrentReads(t *testing.T) {
	marks := []Mark{{Min: []KeyPart{{Value: 1}}, Max: []KeyPart{{Value: 4}}}}
	index, err := Build(marks)
	if err != nil {
		t.Fatal(err)
	}
	marks[0].Min[0].Value = 99
	got, err := index.Candidates(Range{HasLower: true, Lower: []KeyPart{{Value: 1}}}, nil)
	if err != nil || !reflect.DeepEqual(got, []int{0}) {
		t.Fatalf("index changed after input mutation: %v, %v", got, err)
	}
	var group sync.WaitGroup
	for i := 0; i < 8; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for j := 0; j < 1000; j++ {
				if !index.MayOverlap(0, Range{HasUpper: true, Upper: []KeyPart{{Value: 2}}}) {
					t.Errorf("MayOverlap returned false")
					return
				}
			}
		}()
	}
	group.Wait()
}

func TestIndexFallsBackForUnorderedMarks(t *testing.T) {
	index, err := Build([]Mark{
		{Min: []KeyPart{{Value: 10}}, Max: []KeyPart{{Value: 20}}},
		{Min: []KeyPart{{Value: 1}}, Max: []KeyPart{{Value: 2}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := index.Candidates(Range{HasLower: true, Lower: []KeyPart{{Value: 1}}, HasUpper: true, Upper: []KeyPart{{Value: 1}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []int{1}) {
		t.Fatalf("unordered Candidates = %v, want [1]", got)
	}
}
