//go:build t218

package hatDataStructure_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func t218CompareString(left, right string) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}

func t218Index(t testing.TB) *hatDataStructure.MultiPartTreeIndex[t218Record, string] {
	t.Helper()
	index, err := hatDataStructure.NewMultiPartTreeIndex(
		func(record t218Record) []string {
			return []string{record.Region, "order-" + stringIndex(record.Order)}
		},
		t218CompareString,
		2,
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	return index
}

func stringIndex(value int) string {
	return fmt.Sprintf("%05d", value)
}

func TestMultiPartTreeIndexPrefixAndRange(t *testing.T) {
	index := t218Index(t)
	for _, record := range []t218Record{
		{ID: 4, Region: "emea", Order: 4},
		{ID: 2, Region: "apac", Order: 2},
		{ID: 1, Region: "apac", Order: 1},
		{ID: 3, Region: "apac", Order: 3},
		{ID: 5, Region: "emea", Order: 5},
	} {
		if err := index.Upsert(record.ID, record); err != nil {
			t.Fatal(err)
		}
	}

	prefix, ok, err := index.Prefix([]string{"apac"})
	if err != nil || !ok {
		t.Fatalf("Prefix() = %v, %v", ok, err)
	}
	var prefixIDs []uint64
	for {
		entry, next, err := prefix.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !next {
			break
		}
		prefixIDs = append(prefixIDs, entry.ID)
	}
	if want := []uint64{1, 2, 3}; !reflect.DeepEqual(prefixIDs, want) {
		t.Fatalf("prefix IDs = %v, want %v", prefixIDs, want)
	}

	rangeIterator, ok, err := index.Range([]string{"apac", "order-00002"}, []string{"apac", "order-00003"})
	if err != nil || !ok {
		t.Fatalf("Range() = %v, %v", ok, err)
	}
	var rangeIDs []uint64
	for {
		entry, next, err := rangeIterator.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !next {
			break
		}
		rangeIDs = append(rangeIDs, entry.ID)
	}
	if want := []uint64{2, 3}; !reflect.DeepEqual(rangeIDs, want) {
		t.Fatalf("range IDs = %v, want %v", rangeIDs, want)
	}
}

func TestMultiPartTreeIndexMutationAndValidation(t *testing.T) {
	index := t218Index(t)
	if err := index.Upsert(1, t218Record{ID: 1, Region: "apac", Order: 1}); err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(2, t218Record{ID: 2, Region: "apac", Order: 2}); err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(1, t218Record{ID: 1, Region: "emea", Order: 1}); err != nil {
		t.Fatal(err)
	}
	if !index.Delete(2) || index.Delete(99) {
		t.Fatal("Delete() presence result is incorrect")
	}
	if got := index.Len(); got != 1 {
		t.Fatalf("Len() = %d, want 1", got)
	}
	if _, ok, err := index.Prefix([]string{"missing"}); err != nil || ok {
		t.Fatalf("missing prefix = %v, %v", ok, err)
	}
	if _, _, err := index.Prefix([]string{"too", "many", "parts"}); !errors.Is(err, hatDataStructure.ErrMultiPartTreeIndexPrefixTooLong) {
		t.Fatalf("long prefix error = %v", err)
	}
	if _, _, err := index.Range([]string{"apac"}, []string{"emea"}); !errors.Is(err, hatDataStructure.ErrMultiPartTreeIndexKeyPartCount) {
		t.Fatalf("short range error = %v", err)
	}
}

func TestMultiPartTreeIndexInvalidatesIteratorOnMutation(t *testing.T) {
	index := t218Index(t)
	if err := index.Upsert(1, t218Record{ID: 1, Region: "apac", Order: 1}); err != nil {
		t.Fatal(err)
	}
	iterator, ok, err := index.Prefix([]string{"apac"})
	if err != nil || !ok {
		t.Fatalf("Prefix() = %v, %v", ok, err)
	}
	if err := index.Upsert(2, t218Record{ID: 2, Region: "apac", Order: 2}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := iterator.Next(); !errors.Is(err, hatDataStructure.ErrOrderedIndexIteratorInvalidated) {
		t.Fatalf("iterator mutation error = %v", err)
	}
}
