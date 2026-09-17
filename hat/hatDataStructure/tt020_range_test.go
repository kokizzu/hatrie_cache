package hatDataStructure_test

import (
	"reflect"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

type tt020CompositeKey struct {
	Region string
	Stamp  int
}

type tt020CompositeValue struct {
	Key tt020CompositeKey
	ID  uint64
}

func tt020CompareCompositeKey(left, right tt020CompositeKey) int {
	if left.Region < right.Region {
		return -1
	}
	if left.Region > right.Region {
		return 1
	}
	if left.Stamp < right.Stamp {
		return -1
	}
	if left.Stamp > right.Stamp {
		return 1
	}
	return 0
}

func TestTT020OrderedIndexRangeScansInclusiveCompositeBounds(t *testing.T) {
	index, err := hatDataStructure.NewOrderedIndex(
		func(value tt020CompositeValue) tt020CompositeKey { return value.Key },
		tt020CompareCompositeKey,
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	values := []tt020CompositeValue{
		{Key: tt020CompositeKey{Region: "ap", Stamp: 1}, ID: 1},
		{Key: tt020CompositeKey{Region: "eu", Stamp: 1}, ID: 2},
		{Key: tt020CompositeKey{Region: "eu", Stamp: 2}, ID: 3},
		{Key: tt020CompositeKey{Region: "eu", Stamp: 3}, ID: 4},
		{Key: tt020CompositeKey{Region: "us", Stamp: 1}, ID: 5},
	}
	for _, value := range values {
		if err := index.Upsert(value.ID, value); err != nil {
			t.Fatal(err)
		}
	}

	iterator, ok := index.Range(
		tt020CompositeKey{Region: "eu", Stamp: 1},
		tt020CompositeKey{Region: "eu", Stamp: 3},
	)
	if !ok {
		t.Fatal("Range() = false")
	}
	var got []uint64
	for {
		entry, next, err := iterator.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !next {
			break
		}
		got = append(got, entry.Value.ID)
	}
	if want := []uint64{2, 3, 4}; !reflect.DeepEqual(got, want) {
		t.Fatalf("range IDs = %v, want %v", got, want)
	}
	if _, ok := index.Range(
		tt020CompositeKey{Region: "us", Stamp: 2},
		tt020CompositeKey{Region: "us", Stamp: 1},
	); ok {
		t.Fatal("Range() accepted inverted bounds")
	}
}
