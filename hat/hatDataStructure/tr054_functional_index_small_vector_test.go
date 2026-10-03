package hatDataStructure

import (
	"reflect"
	"sync"
	"testing"
)

type tr054FunctionalValue struct {
	Key  int
	Name string
}

func TestTR054FunctionalIndexSmallVector(t *testing.T) {
	index, err := NewFunctionalIndex(func(value tr054FunctionalValue) int { return value.Key }, 256)
	if err != nil {
		t.Fatal(err)
	}
	if index.entries != nil || index.postings != nil {
		t.Fatal("small functional index allocated maps before promotion")
	}

	values := []tr054FunctionalValue{
		{Key: 1, Name: "one"},
		{Key: 1, Name: "two"},
		{Key: 2, Name: "three"},
	}
	for id, value := range values {
		if err := index.Upsert(uint64(id+1), value); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := index.LookupIDs(1), []uint64{1, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("initial key order = %v, want %v", got, want)
	}

	if err := index.Upsert(1, tr054FunctionalValue{Key: 2, Name: "one-moved"}); err != nil {
		t.Fatal(err)
	}
	if got, want := index.LookupIDs(1), []uint64{2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("key order after move = %v, want %v", got, want)
	}
	if got, want := index.LookupIDs(2), []uint64{3, 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("destination order after move = %v, want %v", got, want)
	}

	if err := index.Upsert(1, tr054FunctionalValue{Key: 2, Name: "one-updated"}); err != nil {
		t.Fatal(err)
	}
	if got, want := index.Lookup(2), []tr054FunctionalValue{{Key: 2, Name: "three"}, {Key: 2, Name: "one-updated"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("same-key replacement = %v, want %v", got, want)
	}
	if !index.Delete(3) {
		t.Fatal("Delete(3) = false, want true")
	}
	if got, want := index.LookupIDs(2), []uint64{1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("key order after delete = %v, want %v", got, want)
	}

	for id := uint64(4); id <= 18; id++ {
		if err := index.Upsert(id, tr054FunctionalValue{Key: int(id % 3), Name: "promoted"}); err != nil {
			t.Fatal(err)
		}
	}
	if index.entries == nil || index.postings == nil {
		t.Fatal("functional index did not promote after exceeding the small-vector threshold")
	}
	if got, want := index.Len(), 17; got != want {
		t.Fatalf("promoted length = %d, want %d", got, want)
	}

	index.Clear()
	if index.entries != nil || index.postings != nil || index.Len() != 0 || index.DistinctKeys() != 0 {
		t.Fatalf("Clear did not return to the empty small representation: entries=%v postings=%v len=%d keys=%d", index.entries, index.postings, index.Len(), index.DistinctKeys())
	}
	if err := index.Upsert(0, tr054FunctionalValue{Key: 7, Name: "zero"}); err != nil {
		t.Fatal(err)
	}
	if got, want := index.LookupIDs(7), []uint64{0}; !reflect.DeepEqual(got, want) {
		t.Fatalf("zero ID lookup = %v, want %v", got, want)
	}
}

func TestTR054FunctionalIndexConditionalSmallVector(t *testing.T) {
	index, err := NewConditionalFunctionalIndex(
		func(value tr054FunctionalValue) int { return value.Key },
		func(value tr054FunctionalValue) bool { return value.Key >= 0 },
		256,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(1, tr054FunctionalValue{Key: 2, Name: "admitted"}); err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(2, tr054FunctionalValue{Key: -1, Name: "rejected"}); err != nil {
		t.Fatal(err)
	}
	if !index.Contains(2, 1) || index.Contains(2, 2) {
		t.Fatal("conditional small-vector membership is incorrect")
	}
	if err := index.Upsert(1, tr054FunctionalValue{Key: -1, Name: "removed"}); err != nil {
		t.Fatal(err)
	}
	if index.Len() != 0 {
		t.Fatalf("conditional rejected replacement left %d rows", index.Len())
	}
}

func TestTR054FunctionalIndexConcurrent(t *testing.T) {
	index, err := NewFunctionalIndex(func(value tr054FunctionalValue) int { return value.Key }, 0)
	if err != nil {
		t.Fatal(err)
	}

	var waitGroup sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		worker := worker
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			for offset := 0; offset < 24; offset++ {
				id := uint64(worker*100 + offset)
				if err := index.Upsert(id, tr054FunctionalValue{Key: offset % 4, Name: "concurrent"}); err != nil {
					t.Errorf("Upsert(%d) error: %v", id, err)
				}
				_ = index.LookupIDs(offset % 4)
			}
		}()
	}
	waitGroup.Wait()
	if got, want := index.Len(), 96; got != want {
		t.Fatalf("concurrent length = %d, want %d", got, want)
	}
}
