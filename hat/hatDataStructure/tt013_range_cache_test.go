package hatDataStructure

import "testing"

func TestTT013OrderedIndexRangeCacheHitsAndInvalidates(t *testing.T) {
	index, err := newTT013TestIndex()
	if err != nil {
		t.Fatal(err)
	}
	cache, err := NewOrderedIndexRangeCache(index, 2)
	if err != nil {
		t.Fatal(err)
	}
	destination := make([]OrderedIndexEntry[int, int], 0, 4)
	got, found := cache.RangeInto(2, 3, destination)
	if !found || len(got) != 2 || got[0].Value != 2 || got[1].Value != 3 {
		t.Fatalf("first range = %#v/%t", got, found)
	}
	stats := cache.Stats()
	if stats.Misses != 1 || stats.Hits != 0 || stats.Entries != 1 {
		t.Fatalf("after first range stats = %#v", stats)
	}
	got, found = cache.RangeInto(2, 3, got[:0])
	if !found || len(got) != 2 || got[0].Value != 2 || got[1].Value != 3 {
		t.Fatalf("cached range = %#v/%t", got, found)
	}
	stats = cache.Stats()
	if stats.Misses != 1 || stats.Hits != 1 {
		t.Fatalf("after cache hit stats = %#v", stats)
	}
	if err := index.Upsert(2, 2); err != nil {
		t.Fatal(err)
	}
	got, found = cache.RangeInto(2, 3, got[:0])
	if !found || len(got) != 3 || got[0].Value != 2 || got[1].Value != 2 || got[2].Value != 3 {
		t.Fatalf("invalidated range = %#v/%t", got, found)
	}
	stats = cache.Stats()
	if stats.Hits != 1 || stats.Misses != 2 {
		t.Fatalf("after invalidation stats = %#v", stats)
	}
	if _, found = cache.RangeInto(4, 1, got[:0]); found {
		t.Fatal("inverted range was found")
	}
}

func TestTT013OrderedIndexRangeCacheEvictsOldestSlot(t *testing.T) {
	index, err := newTT013TestIndex()
	if err != nil {
		t.Fatal(err)
	}
	cache, err := NewOrderedIndexRangeCache(index, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, bounds := range [][2]int{{1, 1}, {2, 2}, {3, 3}} {
		if _, found := cache.Range(bounds[0], bounds[1]); !found {
			t.Fatalf("Range(%v) = false", bounds)
		}
	}
	stats := cache.Stats()
	if stats.Entries != 2 || stats.Values != 2 {
		t.Fatalf("eviction stats = %#v", stats)
	}
	if _, found := cache.Range(1, 1); !found {
		t.Fatal("evicted range was not recomputed")
	}
	if got := cache.Stats().Misses; got != 4 {
		t.Fatalf("misses after eviction = %d, want 4", got)
	}
}

func TestTT013OrderedIndexRangeCacheValidation(t *testing.T) {
	if _, err := NewOrderedIndexRangeCache[int, int](nil, 1); err != ErrOrderedIndexRangeCacheIndexRequired {
		t.Fatalf("nil index error = %v", err)
	}
	index, err := newTT013TestIndex()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewOrderedIndexRangeCache(index, 0); err != ErrOrderedIndexRangeCacheCapacityInvalid {
		t.Fatalf("zero capacity error = %v", err)
	}
}

func newTT013TestIndex() (*OrderedIndex[int, int], error) {
	index, err := NewOrderedIndex(
		func(value int) int { return value },
		func(left, right int) int {
			if left < right {
				return -1
			}
			if left > right {
				return 1
			}
			return 0
		},
		0,
	)
	if err != nil {
		return nil, err
	}
	for value := 0; value < 5; value++ {
		if err := index.Upsert(uint64(value+1), value); err != nil {
			return nil, err
		}
	}
	return index, nil
}
