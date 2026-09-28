package hatDataStructure

import (
	"errors"
	"math"
	"testing"
)

func TestM212LogicalCompactionFoldsHistoryThroughFrontier(t *testing.T) {
	compaction := NewLogicalCompaction[string]()
	for _, update := range []struct {
		data string
		time uint64
		diff int64
	}{{"a", 1, 2}, {"a", 4, -1}, {"b", 2, 3}, {"a", 12, 5}} {
		if err := compaction.Add(update.data, update.time, update.diff); err != nil {
			t.Fatalf("Add(%#v) error = %v", update, err)
		}
	}

	removed, err := compaction.CompactThrough(10)
	if err != nil {
		t.Fatalf("CompactThrough() error = %v", err)
	}
	if removed != 3 || compaction.Since() != 10 || compaction.Len() != 3 {
		t.Fatalf("after compaction removed=%d since=%d len=%d", removed, compaction.Since(), compaction.Len())
	}

	got := make(map[DifferentialRecord[string]]struct{})
	compaction.ForEach(func(record DifferentialRecord[string]) { got[record] = struct{}{} })
	for _, want := range []DifferentialRecord[string]{{Data: "a", Time: 10, Diff: 1}, {Data: "b", Time: 10, Diff: 3}, {Data: "a", Time: 12, Diff: 5}} {
		if _, ok := got[want]; !ok {
			t.Fatalf("compacted records = %#v, missing %#v", got, want)
		}
	}
	if err := compaction.Add("late", 9, 1); !errors.Is(err, ErrLogicalCompactionBeforeSince) {
		t.Fatalf("historical Add() error = %v, want ErrLogicalCompactionBeforeSince", err)
	}
}

func TestM212LogicalCompactionIsAtomicOnOverflow(t *testing.T) {
	compaction := NewLogicalCompaction[string]()
	if err := compaction.Add("a", 1, math.MaxInt64); err != nil {
		t.Fatal(err)
	}
	if err := compaction.Add("a", 2, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := compaction.CompactThrough(3); !errors.Is(err, ErrLogicalCompactionOverflow) {
		t.Fatalf("overflow error = %v, want ErrLogicalCompactionOverflow", err)
	}
	if compaction.Since() != 0 || compaction.Len() != 2 {
		t.Fatalf("failed compaction mutated state: since=%d len=%d", compaction.Since(), compaction.Len())
	}
}

func TestM212LogicalCompactionZeroAndNilBehavior(t *testing.T) {
	var nilCompaction *LogicalCompaction[string]
	if err := nilCompaction.Add("a", 1, 1); !errors.Is(err, ErrLogicalCompactionNil) {
		t.Fatalf("nil Add() error = %v", err)
	}
	if _, err := nilCompaction.CompactThrough(1); !errors.Is(err, ErrLogicalCompactionNil) {
		t.Fatalf("nil CompactThrough() error = %v", err)
	}

	compaction := NewLogicalCompaction[string]()
	if err := compaction.Add("a", 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := compaction.Add("a", 1, -1); err != nil {
		t.Fatal(err)
	}
	if compaction.Len() != 0 {
		t.Fatalf("zero-diff entry remained: len=%d", compaction.Len())
	}
	if removed, err := compaction.CompactThrough(5); err != nil || removed != 0 || compaction.Since() != 5 {
		t.Fatalf("empty frontier advance = removed %d, err %v, since %d", removed, err, compaction.Since())
	}
	if _, err := compaction.CompactThrough(4); !errors.Is(err, ErrLogicalCompactionRegression) {
		t.Fatalf("frontier regression error = %v, want ErrLogicalCompactionRegression", err)
	}
}
