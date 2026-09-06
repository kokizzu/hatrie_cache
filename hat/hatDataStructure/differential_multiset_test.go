package hatDataStructure_test

import (
	"errors"
	"testing"

	hatDataStructure "hatrie_cache/hat/hatDataStructure"
)

func TestDifferentialMultisetConsolidatesDataAndTimeDiffs(t *testing.T) {
	multiset := hatDataStructure.NewDifferentialMultiset[string]()
	if err := multiset.Add("a", 1, 3); err != nil {
		t.Fatalf("Add(a,+3) error = %v", err)
	}
	if err := multiset.Add("a", 1, -1); err != nil {
		t.Fatalf("Add(a,-1) error = %v", err)
	}
	if err := multiset.Add("a", 2, 4); err != nil {
		t.Fatalf("Add(a,time2,+4) error = %v", err)
	}
	if err := multiset.Add("zero", 1, 0); err != nil {
		t.Fatalf("Add(zero) error = %v", err)
	}
	if got := multiset.Get("a", 1); got != 2 {
		t.Fatalf("Get(a,1) = %d, want 2", got)
	}
	if multiset.Len() != 2 {
		t.Fatalf("Len() = %d, want 2", multiset.Len())
	}
	if err := multiset.Add("a", 1, -2); err != nil {
		t.Fatalf("Add(a,-2) error = %v", err)
	}
	if multiset.Get("a", 1) != 0 || multiset.Len() != 1 {
		t.Fatalf("after cancellation Get/Len = %d/%d, want 0/1", multiset.Get("a", 1), multiset.Len())
	}

	seen := map[string]int64{}
	multiset.ForEach(func(record hatDataStructure.DifferentialRecord[string]) {
		seen[record.Data] = record.Diff
	})
	if len(seen) != 1 || seen["a"] != 4 {
		t.Fatalf("ForEach() = %#v, want a=4", seen)
	}
}

func TestDifferentialMultisetRejectsOverflowWithoutMutation(t *testing.T) {
	multiset := hatDataStructure.NewDifferentialMultiset[string]()
	if err := multiset.Add("max", 1, int64(^uint64(0)>>1)); err != nil {
		t.Fatalf("Add(max) error = %v", err)
	}
	if err := multiset.Add("max", 1, 1); !errors.Is(err, hatDataStructure.ErrDifferentialOverflow) {
		t.Fatalf("Add(max,+1) error = %v, want ErrDifferentialOverflow", err)
	}
	if got := multiset.Get("max", 1); got != int64(^uint64(0)>>1) {
		t.Fatalf("Get(max) after overflow = %d, want max", got)
	}
	if err := multiset.Add("min", 1, -int64(^uint64(0)>>1)-1); err != nil {
		t.Fatalf("Add(min) error = %v", err)
	}
	if err := multiset.Add("min", 1, -1); !errors.Is(err, hatDataStructure.ErrDifferentialOverflow) {
		t.Fatalf("Add(min,-1) error = %v, want ErrDifferentialOverflow", err)
	}
}

func TestDifferentialMultisetRejectsNilReceiver(t *testing.T) {
	var multiset *hatDataStructure.DifferentialMultiset[string]
	if !errors.Is(multiset.Add("key", 1, 1), hatDataStructure.ErrDifferentialMultisetInvalid) {
		t.Fatalf("nil Add() error = %v, want ErrDifferentialMultisetInvalid", multiset.Add("key", 1, 1))
	}
}

func BenchmarkDifferentialMultisetAdd(b *testing.B) {
	multiset := hatDataStructure.NewDifferentialMultiset[int]()
	b.ReportAllocs()
	for index := range b.N {
		if err := multiset.Add(index%1024, uint64(index), 1); err != nil {
			b.Fatal(err)
		}
	}
}
