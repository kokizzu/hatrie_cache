package hatDataStructure

import (
	"errors"
	"reflect"
	"sync"
	"testing"
)

type multikeyCompositeKey struct {
	Tenant string
	Code   int64
}

type multikeyTypedRecord struct {
	Tags []string
}

func TestMultikeyIndexTypedSetLookupAndDelete(t *testing.T) {
	index := NewMultikeyIndex[int64](MultikeyIndexOptions{MaxKeysPerItem: 3, MaxItems: 2})
	if err := index.Set(1, []int64{3, 1, 3}); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	if err := index.Set(1, []int64{3, 1}); err != nil {
		t.Fatalf("same-key Set() error = %v", err)
	}
	if got := index.Lookup(3, nil); !reflect.DeepEqual(got, []uint64{1}) {
		t.Fatalf("Lookup(3) = %v, want [1]", got)
	}
	if got := index.Lookup(1, []uint64{99}); !reflect.DeepEqual(got, []uint64{1}) {
		t.Fatalf("Lookup(1) with reusable dst = %v, want [1]", got)
	}
	if err := index.Set(2, []int64{2}); err != nil {
		t.Fatalf("second Set() error = %v", err)
	}
	if got := index.Lookup(2, nil); !reflect.DeepEqual(got, []uint64{2}) {
		t.Fatalf("Lookup(2) = %v, want [2]", got)
	}
	if err := index.Set(1, []int64{4, 1}); err != nil {
		t.Fatalf("replacement Set() error = %v", err)
	}
	if index.Contains(3, 1) || !index.Contains(4, 1) {
		t.Fatal("replacement did not move all postings")
	}
	if !index.Delete(1) || index.Delete(1) {
		t.Fatal("Delete() presence result is incorrect")
	}
	if index.Len() != 1 || index.KeyCount() != 1 {
		t.Fatalf("counts after delete = %d/%d, want 1/1", index.Len(), index.KeyCount())
	}
}

func TestMultikeyIndexTypedBoundsAreAtomic(t *testing.T) {
	index := NewMultikeyIndex[int64](MultikeyIndexOptions{MaxKeysPerItem: 2, MaxItems: 1})
	if err := index.Set(1, []int64{1}); err != nil {
		t.Fatalf("initial Set() error = %v", err)
	}
	if err := index.Set(1, []int64{2, 3, 4}); !errors.Is(err, ErrMultikeyIndexKeyLimit) {
		t.Fatalf("key limit error = %v, want key-limit error", err)
	}
	if index.Contains(2, 1) || !index.Contains(1, 1) {
		t.Fatal("key-limit rejection changed existing postings")
	}
	if err := index.Set(2, []int64{2}); !errors.Is(err, ErrMultikeyIndexItemLimit) {
		t.Fatalf("item limit error = %v, want item-limit error", err)
	}
	if index.Len() != 1 || index.Contains(2, 2) {
		t.Fatal("item-limit rejection changed state")
	}
	if err := index.Set(1, nil); err != nil {
		t.Fatalf("empty Set() error = %v", err)
	}
	if index.Len() != 0 || index.KeyCount() != 0 {
		t.Fatalf("empty Set() counts = %d/%d, want 0/0", index.Len(), index.KeyCount())
	}
}

func TestMultikeyIndexSupportsCompositeComparableKeys(t *testing.T) {
	index := NewMultikeyIndex[multikeyCompositeKey](MultikeyIndexOptions{})
	key := multikeyCompositeKey{Tenant: "west", Code: 7}
	if err := index.Set(42, []multikeyCompositeKey{key, key}); err != nil {
		t.Fatalf("composite Set() error = %v", err)
	}
	if got := index.Lookup(key, nil); !reflect.DeepEqual(got, []uint64{42}) {
		t.Fatalf("composite Lookup() = %v, want [42]", got)
	}
}

func TestMultikeyIndexTypedExtractorMaintainsNestedKeys(t *testing.T) {
	index, err := NewTypedMultikeyIndex(func(record multikeyTypedRecord) []string { return record.Tags }, MultikeyIndexOptions{MaxKeysPerItem: 3})
	if err != nil {
		t.Fatalf("NewTypedMultikeyIndex() error = %v", err)
	}
	if err := index.Upsert(1, multikeyTypedRecord{Tags: []string{"red", "large", "red"}}); err != nil {
		t.Fatalf("typed Upsert() error = %v", err)
	}
	if got := index.Lookup("red", nil); !reflect.DeepEqual(got, []uint64{1}) {
		t.Fatalf("typed Lookup(red) = %v, want [1]", got)
	}
	if err := index.Upsert(1, multikeyTypedRecord{Tags: []string{"blue"}}); err != nil {
		t.Fatalf("typed replacement error = %v", err)
	}
	if index.Contains("red", 1) || !index.Contains("blue", 1) {
		t.Fatal("typed replacement did not move postings")
	}
}

func TestMultikeyIndexTypedExtractorRequiresFunction(t *testing.T) {
	if _, err := NewTypedMultikeyIndex[multikeyTypedRecord, string](nil, MultikeyIndexOptions{}); !errors.Is(err, ErrTypedMultikeyIndexExtractorRequired) {
		t.Fatalf("nil extractor error = %v, want extractor error", err)
	}
}

func TestMultikeyIndexNilClearAndConcurrentAccess(t *testing.T) {
	var nilIndex *MultikeyIndex[int64]
	if err := nilIndex.Set(1, []int64{1}); !errors.Is(err, ErrMultikeyIndexNil) {
		t.Fatalf("nil Set() error = %v, want nil error", err)
	}
	if got := nilIndex.Lookup(1, []uint64{9}); len(got) != 0 {
		t.Fatalf("nil Lookup() = %v, want empty", got)
	}

	index := NewMultikeyIndex[int64](MultikeyIndexOptions{MaxKeysPerItem: 4, MaxItems: 64})
	var wait sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		worker := worker
		wait.Add(1)
		go func() {
			defer wait.Done()
			id := uint64(worker + 1)
			keys := []int64{int64(worker), int64(worker + 16)}
			for iteration := 0; iteration < 50; iteration++ {
				if err := index.Set(id, keys); err != nil {
					t.Errorf("concurrent Set() error = %v", err)
					return
				}
				_ = index.Lookup(keys[0], nil)
			}
		}()
	}
	wait.Wait()
	if index.Len() != 16 {
		t.Fatalf("Len() after concurrent Set() = %d, want 16", index.Len())
	}
	index.Clear()
	if index.Len() != 0 || index.KeyCount() != 0 {
		t.Fatalf("counts after Clear() = %d/%d, want 0/0", index.Len(), index.KeyCount())
	}
}
