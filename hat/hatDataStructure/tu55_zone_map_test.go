package hatDataStructure

import (
	"testing"
)

func TestTU55SkipsNonMatchingBlocks(t *testing.T) {
	index, err := NewZoneMapIndex(3, func(left, right int) bool {
		return left < right
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Build([]int{1, 2, 3, 100, 101, 102, 7, 8}); err != nil {
		t.Fatal(err)
	}
	if index.RowCount() != 8 || index.SegmentCount() != 3 || index.SegmentSize() != 3 {
		t.Fatalf("index dimensions = rows %d segments %d size %d", index.RowCount(), index.SegmentCount(), index.SegmentSize())
	}

	var candidates []ZoneMapSegment[int]
	index.VisitEqual(101, func(segment ZoneMapSegment[int]) bool {
		candidates = append(candidates, segment)
		return true
	})
	if len(candidates) != 1 || candidates[0].Start != 3 || candidates[0].End != 6 {
		t.Fatalf("equal candidates = %+v", candidates)
	}

	candidates = candidates[:0]
	index.VisitRange(6, 8, func(segment ZoneMapSegment[int]) bool {
		candidates = append(candidates, segment)
		return true
	})
	if len(candidates) != 1 || candidates[0].Start != 6 || candidates[0].End != 8 {
		t.Fatalf("range candidates = %+v", candidates)
	}
}

func TestTU55SupportsEarlyStopAndEmptyRanges(t *testing.T) {
	index, err := NewZoneMapIndex(2, func(left, right int) bool { return left < right })
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Build([]int{1, 9, 3, 7, 20}); err != nil {
		t.Fatal(err)
	}

	visited := 0
	index.VisitRange(0, 30, func(segment ZoneMapSegment[int]) bool {
		visited++
		return false
	})
	if visited != 1 {
		t.Fatalf("early-stop visits = %d", visited)
	}

	visited = 0
	index.VisitRange(10, 5, func(segment ZoneMapSegment[int]) bool {
		visited++
		return true
	})
	if visited != 0 {
		t.Fatalf("reversed range visits = %d", visited)
	}

	if got := index.VisitEqual(100, nil); got != 0 {
		t.Fatalf("nil visitor count = %d", got)
	}
}

func TestTU55RejectsInvalidConfigurationAndKeepsBuildAtomic(t *testing.T) {
	if _, err := NewZoneMapIndex(0, func(left, right int) bool { return left < right }); err != ErrZoneMapSegmentSize {
		t.Fatalf("zero segment size error = %v", err)
	}
	if _, err := NewZoneMapIndex[int](2, nil); err != ErrZoneMapComparator {
		t.Fatalf("nil comparator error = %v", err)
	}

	index, err := NewZoneMapIndex(2, func(left, right int) bool { return left < right })
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Build([]int{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if err := index.Build(nil); err != nil {
		t.Fatal(err)
	}
	if index.SegmentCount() != 0 || index.RowCount() != 0 {
		t.Fatalf("empty rebuild dimensions = %d, %d", index.SegmentCount(), index.RowCount())
	}
}
