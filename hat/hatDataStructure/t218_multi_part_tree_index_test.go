//go:build t218

package hatDataStructure

import (
	"errors"
	"testing"
)

type t218CompositeKey struct {
	Prefix string
	Value  int64
}

func compareT218CompositeKey(left, right t218CompositeKey) int {
	if left.Prefix < right.Prefix {
		return -1
	}
	if left.Prefix > right.Prefix {
		return 1
	}
	if left.Value < right.Value {
		return -1
	}
	if left.Value > right.Value {
		return 1
	}
	return 0
}

func TestT218MultiPartTreeIndexMergesPrefixAndRangeScans(t *testing.T) {
	index, err := NewMultiPartTreeIndex[int, t218CompositeKey](compareT218CompositeKey)
	if err != nil {
		t.Fatal(err)
	}
	firstPart, err := index.AddPart([]TreeIndexEntry[int, t218CompositeKey]{
		{ID: 1, Key: t218CompositeKey{Prefix: "apac", Value: 3}, Value: 13},
		{ID: 2, Key: t218CompositeKey{Prefix: "eu", Value: 2}, Value: 22},
		{ID: 3, Key: t218CompositeKey{Prefix: "apac", Value: 1}, Value: 11},
	})
	if err != nil {
		t.Fatal(err)
	}
	secondPart, err := index.AddPart([]TreeIndexEntry[int, t218CompositeKey]{
		{ID: 4, Key: t218CompositeKey{Prefix: "apac", Value: 2}, Value: 12},
		{ID: 5, Key: t218CompositeKey{Prefix: "eu", Value: 1}, Value: 21},
		{ID: 6, Key: t218CompositeKey{Prefix: "eu", Value: 3}, Value: 23},
	})
	if err != nil {
		t.Fatal(err)
	}
	if firstPart == secondPart || index.PartCount() != 2 || index.Len() != 6 {
		t.Fatalf("parts/len = %d/%d, want two distinct parts and six entries", index.PartCount(), index.Len())
	}

	iterator, found := index.Prefix(
		t218CompositeKey{Prefix: "apac", Value: -1 << 63},
		t218CompositeKey{Prefix: "apac", Value: 1<<63 - 1},
	)
	if !found {
		t.Fatal("Prefix() found = false, want true")
	}
	defer iterator.Close()
	var ids []uint64
	var values []int
	for {
		entry, next, err := iterator.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !next {
			break
		}
		ids = append(ids, entry.ID)
		values = append(values, entry.Value)
	}
	if got, want := len(ids), 3; got != want {
		t.Fatalf("prefix rows = %d, want %d", got, want)
	}
	for index, want := range []uint64{3, 4, 1} {
		if ids[index] != want {
			t.Fatalf("prefix id[%d] = %d, want %d", index, ids[index], want)
		}
	}
	for index, want := range []int{11, 12, 13} {
		if values[index] != want {
			t.Fatalf("prefix value[%d] = %d, want %d", index, values[index], want)
		}
	}

	rangeIterator, found := index.Range(
		t218CompositeKey{Prefix: "apac", Value: 2},
		t218CompositeKey{Prefix: "eu", Value: 2},
	)
	if !found {
		t.Fatal("Range() found = false, want true")
	}
	defer rangeIterator.Close()
	count := 0
	for {
		_, next, err := rangeIterator.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !next {
			break
		}
		count++
	}
	if count != 4 {
		t.Fatalf("bounded range count = %d, want 4", count)
	}

	if !index.DeletePart(firstPart) || index.PartCount() != 1 || index.Len() != 3 {
		t.Fatalf("DeletePart() did not remove the first part")
	}
}

func TestT218MultiPartTreeIndexInvalidatesIteratorsAndValidatesInputs(t *testing.T) {
	if _, err := NewMultiPartTreeIndex[int, int](nil); !errors.Is(err, ErrMultiPartTreeIndexComparatorRequired) {
		t.Fatalf("nil comparator error = %v, want %v", err, ErrMultiPartTreeIndexComparatorRequired)
	}
	index, err := NewMultiPartTreeIndex[int, int](func(left, right int) int { return left - right })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := index.AddPart(nil); !errors.Is(err, ErrMultiPartTreeIndexPartEmpty) {
		t.Fatalf("empty part error = %v, want %v", err, ErrMultiPartTreeIndexPartEmpty)
	}
	if _, found := index.Range(1, 1); found {
		t.Fatal("empty index range found = true, want false")
	}
	if _, err := index.AddPart([]TreeIndexEntry[int, int]{{ID: 1, Key: 1, Value: 1}}); err != nil {
		t.Fatal(err)
	}
	iterator, found := index.Range(1, 1)
	if !found {
		t.Fatal("Range() found = false, want true")
	}
	if _, err := index.AddPart([]TreeIndexEntry[int, int]{{ID: 2, Key: 2, Value: 2}}); err != nil {
		t.Fatal(err)
	}
	if _, next, err := iterator.Next(); !errors.Is(err, ErrMultiPartTreeIndexIteratorInvalidated) || next {
		t.Fatalf("invalidated iterator = next:%t err:%v", next, err)
	}
	iterator.Close()
	if index.DeletePart(999) {
		t.Fatal("DeletePart(missing) = true, want false")
	}
}
