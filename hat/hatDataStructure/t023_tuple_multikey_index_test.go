package hatDataStructure

import (
	"errors"
	"reflect"
	"sync"
	"testing"
)

type t023Record struct {
	Tags []uint32
}

func t023Index(t *testing.T, options TupleMultikeyIndexOptions) *TupleMultikeyIndex[t023Record, uint32] {
	t.Helper()
	index, err := NewTupleMultikeyIndex(func(record t023Record) ([]uint32, error) {
		return record.Tags, nil
	}, options)
	if err != nil {
		t.Fatal(err)
	}
	return index
}

func TestT023TupleMultikeyIndexUpsertLookupAndDelete(t *testing.T) {
	index := t023Index(t, TupleMultikeyIndexOptions{MaxKeysPerItem: 4, MaxItems: 8})
	if err := index.Upsert(2, t023Record{Tags: []uint32{7, 9, 7}}); err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(1, t023Record{Tags: []uint32{9}}); err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(3, t023Record{Tags: []uint32{7}}); err != nil {
		t.Fatal(err)
	}
	if got := index.Lookup(7, nil); !reflect.DeepEqual(got, []uint64{2, 3}) {
		t.Fatalf("tag 7 lookup = %v, want [2 3]", got)
	}
	if got := index.Lookup(9, nil); !reflect.DeepEqual(got, []uint64{1, 2}) {
		t.Fatalf("tag 9 lookup = %v, want [1 2]", got)
	}
	destination := make([]uint64, 0, 4)
	if got := index.Lookup(9, destination); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("reused lookup = %v", got)
	}
	if !index.Contains(7, 2) || index.Contains(7, 1) {
		t.Fatal("Contains returned an incorrect membership result")
	}

	if err := index.Upsert(2, t023Record{Tags: []uint32{11}}); err != nil {
		t.Fatal(err)
	}
	if got := index.Lookup(7, nil); !reflect.DeepEqual(got, []uint64{3}) {
		t.Fatalf("tag 7 after update = %v, want [3]", got)
	}
	if got := index.Lookup(11, nil); !reflect.DeepEqual(got, []uint64{2}) {
		t.Fatalf("tag 11 after update = %v, want [2]", got)
	}
	if !index.Delete(1) || index.Delete(1) {
		t.Fatal("Delete did not report idempotent membership")
	}
	if index.Len() != 2 || index.KeyCount() != 2 {
		t.Fatalf("counts = %d items/%d keys, want 2/2", index.Len(), index.KeyCount())
	}
	index.Clear()
	if index.Len() != 0 || index.KeyCount() != 0 || len(index.Lookup(7, nil)) != 0 {
		t.Fatal("Clear retained tuple multikey state")
	}
}

func TestT023TupleMultikeyIndexRejectsAtomicallyAndBoundsState(t *testing.T) {
	index := t023Index(t, TupleMultikeyIndexOptions{MaxKeysPerItem: 2, MaxItems: 1})
	if err := index.Upsert(7, t023Record{Tags: []uint32{1, 2}}); err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(7, t023Record{Tags: []uint32{1, 2, 3}}); !errors.Is(err, ErrTupleMultikeyIndexLimit) {
		t.Fatalf("key limit error = %v", err)
	}
	if got := index.Lookup(1, nil); !reflect.DeepEqual(got, []uint64{7}) || len(index.Lookup(3, nil)) != 0 {
		t.Fatalf("rejected update changed state: key1=%v key3=%v", got, index.Lookup(3, nil))
	}
	if err := index.Upsert(8, t023Record{Tags: []uint32{4}}); !errors.Is(err, ErrTupleMultikeyIndexLimit) {
		t.Fatalf("item limit error = %v", err)
	}

	extractorErr := errors.New("extractor failed")
	index, err := NewTupleMultikeyIndex(func(t023Record) ([]uint32, error) { return nil, extractorErr }, TupleMultikeyIndexOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(1, t023Record{}); !errors.Is(err, extractorErr) {
		t.Fatalf("extractor error = %v", err)
	}
	if _, err := NewTupleMultikeyIndex[t023Record, uint32](nil, TupleMultikeyIndexOptions{}); !errors.Is(err, ErrTupleMultikeyIndexExtractorRequired) {
		t.Fatalf("nil extractor error = %v", err)
	}
}

func TestT023TupleMultikeyIndexConcurrentUse(t *testing.T) {
	index := t023Index(t, TupleMultikeyIndexOptions{MaxKeysPerItem: 3, MaxItems: 64})
	if err := index.Upsert(0, t023Record{Tags: []uint32{1}}); err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	wait.Add(3)
	go func() {
		defer wait.Done()
		for iteration := 0; iteration < 1000; iteration++ {
			if err := index.Upsert(uint64(iteration%64), t023Record{Tags: []uint32{uint32(iteration % 8)}}); err != nil {
				t.Errorf("concurrent upsert: %v", err)
				return
			}
		}
	}()
	go func() {
		defer wait.Done()
		destination := make([]uint64, 0, 64)
		for iteration := 0; iteration < 1000; iteration++ {
			destination = index.Lookup(uint32(iteration%8), destination)
		}
	}()
	go func() {
		defer wait.Done()
		for iteration := 0; iteration < 1000; iteration++ {
			index.Contains(uint32(iteration%8), uint64(iteration%64))
			index.Len()
			index.KeyCount()
		}
	}()
	wait.Wait()
}
