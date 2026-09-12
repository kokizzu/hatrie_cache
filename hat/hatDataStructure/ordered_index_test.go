package hatDataStructure

import (
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
)

type orderedIndexTestRecord struct {
	Key   string
	Value int
}

func TestOrderedIndexIteratorOrderAndSeek(t *testing.T) {
	index, err := NewOrderedIndex(
		func(record orderedIndexTestRecord) string { return record.Key },
		strings.Compare,
		4,
	)
	if err != nil {
		t.Fatalf("NewOrderedIndex() error = %v", err)
	}
	for id, record := range map[uint64]orderedIndexTestRecord{
		30: {Key: "b", Value: 30},
		10: {Key: "a", Value: 10},
		20: {Key: "b", Value: 20},
	} {
		if err := index.Upsert(id, record); err != nil {
			t.Fatalf("Upsert(%d) error = %v", id, err)
		}
	}

	iterator, ok := index.First()
	if !ok {
		t.Fatal("First() = false, want an iterator")
	}
	var got []OrderedIndexEntry[orderedIndexTestRecord, string]
	for {
		entry, next, err := iterator.Next()
		if err != nil {
			t.Fatalf("iterator.Next() error = %v", err)
		}
		if !next {
			break
		}
		got = append(got, entry)
	}
	want := []OrderedIndexEntry[orderedIndexTestRecord, string]{
		{ID: 10, Key: "a", Value: orderedIndexTestRecord{Key: "a", Value: 10}},
		{ID: 20, Key: "b", Value: orderedIndexTestRecord{Key: "b", Value: 20}},
		{ID: 30, Key: "b", Value: orderedIndexTestRecord{Key: "b", Value: 30}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("First() entries = %#v, want %#v", got, want)
	}

	iterator, ok = index.Seek("b")
	if !ok {
		t.Fatal("Seek(b) = false, want an iterator")
	}
	entry, next, err := iterator.Next()
	if err != nil || !next || entry.ID != 20 {
		t.Fatalf("Seek(b).Next() = %#v, %v, %v, want ID 20", entry, next, err)
	}
	iterator, ok = index.SeekAfter("a")
	if !ok {
		t.Fatal("SeekAfter(a) = false, want an iterator")
	}
	entry, next, err = iterator.Next()
	if err != nil || !next || entry.ID != 20 {
		t.Fatalf("SeekAfter(a).Next() = %#v, %v, %v, want ID 20", entry, next, err)
	}
	if _, ok := index.SeekAfter("b"); ok {
		t.Fatal("SeekAfter(b) = true, want no key after b")
	}
}

func TestOrderedIndexIteratorInvalidatesAfterMutation(t *testing.T) {
	index, err := NewOrderedIndex(func(value int) int { return value }, func(left, right int) int {
		if left < right {
			return -1
		}
		if left > right {
			return 1
		}
		return 0
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(1, 10); err != nil {
		t.Fatal(err)
	}
	iterator, ok := index.First()
	if !ok {
		t.Fatal("First() = false")
	}
	if err := index.Upsert(2, 20); err != nil {
		t.Fatal(err)
	}
	if _, next, err := iterator.Next(); !errors.Is(err, ErrOrderedIndexIteratorInvalidated) || next {
		t.Fatalf("invalidated Next() = next=%v err=%v", next, err)
	}
}

func TestOrderedIndexMutationAndAllocs(t *testing.T) {
	index, err := NewOrderedIndex(func(value int) int { return value }, func(left, right int) int {
		return left - right
	}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(1, 5); err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(1, 3); err != nil {
		t.Fatal(err)
	}
	if got := index.Len(); got != 1 {
		t.Fatalf("Len() = %d, want 1", got)
	}
	if !index.Delete(1) || index.Delete(1) {
		t.Fatal("Delete() presence result is incorrect")
	}
	if _, ok := index.First(); ok {
		t.Fatal("First() = true after delete")
	}
	if err := index.Upsert(2, 8); err != nil {
		t.Fatal(err)
	}
	if got := testing.AllocsPerRun(100, func() {
		if err := index.Upsert(2, 8); err != nil {
			t.Fatal(err)
		}
	}); got != 0 {
		t.Fatalf("inactive mutation allocation count = %f, want 0", got)
	}
	if got := testing.AllocsPerRun(100, func() {
		iterator, ok := index.First()
		if !ok {
			t.Fatal("First() = false")
		}
		if _, next, err := iterator.Next(); err != nil || !next {
			t.Fatalf("Next() = %v, %v", next, err)
		}
	}); got != 0 {
		t.Fatalf("iterator allocation count = %f, want 0", got)
	}
}

func TestOrderedIndexConcurrentTraversalAndMutation(t *testing.T) {
	index, err := NewOrderedIndex(func(value int) int { return value }, func(left, right int) int {
		return left - right
	}, 128)
	if err != nil {
		t.Fatal(err)
	}
	for value := 0; value < 128; value++ {
		if err := index.Upsert(uint64(value+1), value); err != nil {
			t.Fatal(err)
		}
	}

	start := make(chan struct{})
	var wait sync.WaitGroup
	var readerErr error
	var writerErr error
	wait.Add(2)
	go func() {
		defer wait.Done()
		<-start
		for iteration := 0; iteration < 128; iteration++ {
			iterator, ok := index.First()
			if !ok {
				readerErr = errors.New("First() unexpectedly returned no iterator")
				return
			}
			for {
				_, next, err := iterator.Next()
				if errors.Is(err, ErrOrderedIndexIteratorInvalidated) {
					break
				}
				if err != nil {
					readerErr = err
					return
				}
				if !next {
					break
				}
			}
		}
	}()
	go func() {
		defer wait.Done()
		<-start
		for iteration := 0; iteration < 256; iteration++ {
			id := uint64(iteration%128 + 1)
			if err := index.Upsert(id, iteration%128); err != nil {
				writerErr = err
				return
			}
		}
	}()
	close(start)
	wait.Wait()
	if readerErr != nil {
		t.Fatal(readerErr)
	}
	if writerErr != nil {
		t.Fatal(writerErr)
	}
}

func TestOrderedIndexIteratorClose(t *testing.T) {
	index, err := NewOrderedIndex(func(value int) int { return value }, func(left, right int) int {
		return left - right
	}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(1, 1); err != nil {
		t.Fatal(err)
	}
	iterator, ok := index.First()
	if !ok {
		t.Fatal("First() = false")
	}
	iterator.Close()
	if _, next, err := iterator.Next(); !errors.Is(err, ErrOrderedIndexIteratorClosed) || next {
		t.Fatalf("closed iterator Next() = next=%v err=%v", next, err)
	}
}

func TestOrderedIndexRejectsMissingConfiguration(t *testing.T) {
	if _, err := NewOrderedIndex[int, int](nil, func(left, right int) int { return left - right }, 0); !errors.Is(err, ErrOrderedIndexExtractorRequired) {
		t.Fatalf("nil extractor error = %v", err)
	}
	if _, err := NewOrderedIndex[int, int](func(value int) int { return value }, nil, 0); !errors.Is(err, ErrOrderedIndexComparatorRequired) {
		t.Fatalf("nil comparator error = %v", err)
	}
	var index *OrderedIndex[int, int]
	if _, ok := index.Seek(1); ok {
		t.Fatal("nil Seek() = true")
	}
	if err := index.Upsert(1, 1); !errors.Is(err, ErrOrderedIndexNil) {
		t.Fatalf("nil Upsert error = %v", err)
	}
	var iterator OrderedIndexIterator[int, int]
	if _, next, err := iterator.Next(); !errors.Is(err, ErrOrderedIndexIteratorNil) || next {
		t.Fatalf("nil iterator Next() = next=%v err=%v", next, err)
	}
}
