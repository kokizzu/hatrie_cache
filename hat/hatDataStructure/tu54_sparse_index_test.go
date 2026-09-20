package hatDataStructure

import (
	"errors"
	"testing"
)

func TestTU54BuildsBoundedSparseWindows(t *testing.T) {
	index, err := NewSparsePrimaryIndex(3, func(left, right int) bool {
		return left < right
	})
	if err != nil {
		t.Fatal(err)
	}
	keys := []int{1, 3, 3, 8, 9, 12, 20}
	if err := index.Build(keys); err != nil {
		t.Fatal(err)
	}
	if index.RowCount() != 7 || index.AnchorCount() != 3 || index.Stride() != 3 {
		t.Fatalf("index dimensions = rows %d anchors %d stride %d", index.RowCount(), index.AnchorCount(), index.Stride())
	}

	tests := []struct {
		name      string
		key       int
		wantStart uint64
		wantEnd   uint64
	}{
		{name: "before first", key: 0, wantStart: 0, wantEnd: 3},
		{name: "first", key: 1, wantStart: 0, wantEnd: 3},
		{name: "middle", key: 4, wantStart: 0, wantEnd: 3},
		{name: "second anchor", key: 8, wantStart: 3, wantEnd: 6},
		{name: "last block", key: 19, wantStart: 3, wantEnd: 6},
		{name: "last key", key: 20, wantStart: 6, wantEnd: 7},
		{name: "after last", key: 100, wantStart: 6, wantEnd: 7},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			window, ok := index.Window(test.key)
			if !ok {
				t.Fatal("window not available")
			}
			if window.Start != test.wantStart || window.End != test.wantEnd {
				t.Fatalf("window = %+v, want [%d,%d)", window, test.wantStart, test.wantEnd)
			}
		})
	}
}

func TestTU54RejectsInvalidSparseIndexInput(t *testing.T) {
	if _, err := NewSparsePrimaryIndex(0, func(left, right int) bool { return left < right }); !errors.Is(err, ErrSparseIndexStride) {
		t.Fatalf("zero stride error = %v", err)
	}
	if _, err := NewSparsePrimaryIndex[int](3, nil); !errors.Is(err, ErrSparseIndexComparator) {
		t.Fatalf("nil comparator error = %v", err)
	}

	index, err := NewSparsePrimaryIndex(2, func(left, right int) bool { return left < right })
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Build([]int{1, 4, 3}); !errors.Is(err, ErrSparseIndexUnsorted) {
		t.Fatalf("unsorted error = %v", err)
	}
	if _, ok := index.Window(1); ok {
		t.Fatal("empty index returned a window")
	}
	if err := index.Build([]int{1, 4, 8}); err != nil {
		t.Fatal(err)
	}
	if err := index.Build([]int{1, 9, 3}); !errors.Is(err, ErrSparseIndexUnsorted) {
		t.Fatalf("second unsorted error = %v", err)
	}
	window, ok := index.Window(4)
	if !ok || window.Start != 0 || window.End != 2 {
		t.Fatalf("failed rebuild replaced index: %+v, %v", window, ok)
	}
}

func TestTU54BuildCopiesOnlyAnchorsAndSupportsDuplicates(t *testing.T) {
	index, err := NewSparsePrimaryIndex(2, func(left, right string) bool {
		return left < right
	})
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{"a", "a", "b", "c", "c"}
	if err := index.Build(keys); err != nil {
		t.Fatal(err)
	}
	keys[0] = "z"
	window, ok := index.Window("a")
	if !ok || window.Start != 0 || window.End != 2 {
		t.Fatalf("window after source mutation = %+v, %v", window, ok)
	}
	if index.AnchorCount() != 3 {
		t.Fatalf("anchor count = %d", index.AnchorCount())
	}
}
