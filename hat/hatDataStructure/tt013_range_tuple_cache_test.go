//go:build !tt013baseline

package hatDataStructure

import (
	"errors"
	"reflect"
	"testing"
)

func TestTT013RangeTupleCacheClonesAndVersionsRanges(t *testing.T) {
	cache, err := NewRangeTupleCache[int, string](2)
	if err != nil {
		t.Fatalf("NewRangeTupleCache() error = %v", err)
	}
	values := []string{"ada", "lin"}
	if err := cache.Put(10, 20, 7, values); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	values[0] = "mutated"
	got, ok := cache.Get(10, 20, 7)
	if !ok || !reflect.DeepEqual(got, []string{"ada", "lin"}) {
		t.Fatalf("Get() = %#v, %v, want cloned values", got, ok)
	}
	if _, ok := cache.Get(10, 20, 8); ok {
		t.Fatal("Get() returned a stale version")
	}
	if err := cache.Put(10, 20, 8, []string{"new"}); err != nil {
		t.Fatalf("Put(new version) error = %v", err)
	}
	if got, ok := cache.Get(10, 20, 8); !ok || !reflect.DeepEqual(got, []string{"new"}) {
		t.Fatalf("Get(new version) = %#v, %v", got, ok)
	}
}

func TestTT013RangeTupleCacheLRUAndStats(t *testing.T) {
	cache, err := NewRangeTupleCache[int, int](2)
	if err != nil {
		t.Fatalf("NewRangeTupleCache() error = %v", err)
	}
	for key := 1; key <= 2; key++ {
		if err := cache.Put(key, key, 1, []int{key}); err != nil {
			t.Fatalf("Put(%d) error = %v", key, err)
		}
	}
	if _, ok := cache.Get(1, 1, 1); !ok {
		t.Fatal("Get(1) missed before eviction")
	}
	if err := cache.Put(3, 3, 1, []int{3}); err != nil {
		t.Fatalf("Put(3) error = %v", err)
	}
	if _, ok := cache.Get(2, 2, 1); ok {
		t.Fatal("Get(2) hit after LRU eviction")
	}
	if !cache.Invalidate(1, 1) {
		t.Fatal("Invalidate(1) = false, want true")
	}
	if _, ok := cache.Get(1, 1, 1); ok {
		t.Fatal("Get(1) hit after invalidation")
	}
	stats := cache.Stats()
	if stats.Entries != 1 || stats.Hits == 0 || stats.Misses < 2 || stats.Evictions != 1 {
		t.Fatalf("Stats() = %#v, want one entry, hits, misses, and one eviction", stats)
	}
	cache.Clear()
	if got := cache.Stats().Entries; got != 0 {
		t.Fatalf("Entries after Clear() = %d, want 0", got)
	}
}

func TestTT013RangeTupleCacheValidatesCapacityAndNilReceiver(t *testing.T) {
	if _, err := NewRangeTupleCache[int, int](0); !errors.Is(err, ErrRangeTupleCacheCapacityInvalid) {
		t.Fatalf("zero capacity error = %v, want %v", err, ErrRangeTupleCacheCapacityInvalid)
	}
	var cache *RangeTupleCache[int, int]
	if _, ok := cache.Get(1, 2, 1); ok {
		t.Fatal("nil Get() hit")
	}
	if err := cache.Put(1, 2, 1, []int{1}); !errors.Is(err, ErrRangeTupleCacheNil) {
		t.Fatalf("nil Put() error = %v, want %v", err, ErrRangeTupleCacheNil)
	}
}
