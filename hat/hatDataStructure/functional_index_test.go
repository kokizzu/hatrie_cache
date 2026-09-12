package hatDataStructure

import (
	"errors"
	"strings"
	"sync"
	"testing"
)

type functionalIndexPerson struct {
	Name string
	Age  int
}

func TestFunctionalIndexLookupUpdateDeleteAndDuplicateKeys(t *testing.T) {
	index, err := NewFunctionalIndex[functionalIndexPerson, string](func(person functionalIndexPerson) string {
		return strings.ToLower(person.Name)
	}, 4)
	if err != nil {
		t.Fatalf("NewFunctionalIndex() error = %v", err)
	}
	if err := index.Upsert(1, functionalIndexPerson{Name: "Ada", Age: 36}); err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(2, functionalIndexPerson{Name: "ADA", Age: 37}); err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(3, functionalIndexPerson{Name: "Grace", Age: 28}); err != nil {
		t.Fatal(err)
	}

	people := index.Lookup("ada")
	if len(people) != 2 || people[0].Age != 36 || people[1].Age != 37 {
		t.Fatalf("Lookup(ada) = %#v", people)
	}
	if index.Len() != 3 || index.DistinctKeys() != 2 {
		t.Fatalf("lengths = %d entries, %d keys", index.Len(), index.DistinctKeys())
	}

	if err := index.Upsert(1, functionalIndexPerson{Name: "Grace", Age: 39}); err != nil {
		t.Fatal(err)
	}
	if got := index.Lookup("ada"); len(got) != 1 || got[0].Age != 37 {
		t.Fatalf("Lookup(ada) after update = %#v", got)
	}
	if got := index.Lookup("grace"); len(got) != 2 || got[0].Age != 28 || got[1].Age != 39 {
		t.Fatalf("Lookup(grace) after update = %#v", got)
	}
	if !index.Delete(2) || index.Delete(2) {
		t.Fatal("Delete() did not have one-shot semantics")
	}
	if got := index.Lookup("ada"); len(got) != 0 {
		t.Fatalf("Lookup(ada) after delete = %#v", got)
	}
}

func TestFunctionalIndexLookupIntoReusesScratchAndClear(t *testing.T) {
	index, err := NewFunctionalIndex[int, int](func(value int) int { return value % 2 }, 2)
	if err != nil {
		t.Fatal(err)
	}
	for value := 0; value < 4; value++ {
		if err := index.Upsert(uint64(value), value); err != nil {
			t.Fatal(err)
		}
	}
	scratch := make([]int, 0, 4)
	first := index.LookupInto(1, scratch)
	firstPtr := &first[0]
	second := index.LookupInto(0, first)
	if len(second) != 2 || second[0] != 0 || second[1] != 2 {
		t.Fatalf("LookupInto() = %#v", second)
	}
	if &second[0] != firstPtr {
		t.Fatal("LookupInto() did not reuse the supplied scratch backing array")
	}
	index.Clear()
	if index.Len() != 0 || index.DistinctKeys() != 0 || len(index.Lookup(0)) != 0 {
		t.Fatalf("after Clear() lengths=%d/%d lookup=%v", index.Len(), index.DistinctKeys(), index.Lookup(0))
	}
}

func TestFunctionalIndexRejectsMissingExtractorAndNilReceiver(t *testing.T) {
	if _, err := NewFunctionalIndex[int, int](nil, 0); !errors.Is(err, ErrFunctionalIndexExtractorRequired) {
		t.Fatalf("nil extractor error = %v", err)
	}
	var nilIndex *FunctionalIndex[int, int]
	if err := nilIndex.Upsert(1, 1); !errors.Is(err, ErrFunctionalIndexNil) {
		t.Fatalf("nil Upsert() error = %v", err)
	}
	if nilIndex.Delete(1) || nilIndex.Len() != 0 || len(nilIndex.Lookup(1)) != 0 {
		t.Fatal("nil receiver returned state")
	}
}

func TestFunctionalIndexConcurrentMixedOperations(t *testing.T) {
	index, err := NewFunctionalIndex[int, int](func(value int) int { return value % 8 }, 2048)
	if err != nil {
		t.Fatal(err)
	}

	const (
		workers   = 8
		perWorker = 256
	)
	var wait sync.WaitGroup
	wait.Add(workers)
	for worker := 0; worker < workers; worker++ {
		worker := worker
		go func() {
			defer wait.Done()
			for i := 0; i < perWorker; i++ {
				id := uint64(worker*perWorker + i + 1)
				if err := index.Upsert(id, int(id)); err != nil {
					t.Errorf("Upsert() error = %v", err)
					return
				}
				if worker%2 == 0 {
					scratch := make([]uint64, 0, 8)
					_ = index.LookupIDsInto(int(id)%8, scratch)
				} else if i%2 == 0 && !index.Delete(id) {
					t.Errorf("Delete(%d) = false", id)
					return
				}
			}
		}()
	}
	wait.Wait()

	wantLen := workers/2*perWorker + workers/2*(perWorker/2)
	if got := index.Len(); got != wantLen {
		t.Fatalf("Len() = %d, want %d", got, wantLen)
	}
	if got := index.DistinctKeys(); got != 8 {
		t.Fatalf("DistinctKeys() = %d, want 8", got)
	}
}
