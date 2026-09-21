package hatDataStructure_test

import (
	"errors"
	"sync"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestTT018PageIndexResidencyEvictsLeastRecentlyUsedAndTracksStats(t *testing.T) {
	cache, err := hatDataStructure.NewPageIndexResidency[string, string](hatDataStructure.PageIndexResidencyOptions{
		MaxBytes:   8,
		MaxEntries: 2,
	})
	if err != nil {
		t.Fatalf("NewPageIndexResidency: %v", err)
	}
	if !cache.Put("a", "page-a", 4) || !cache.Put("b", "page-b", 4) {
		t.Fatal("initial Put rejected an eligible page")
	}
	if value, ok := cache.Get("a"); !ok || value != "page-a" {
		t.Fatalf("Get(a) = %q, %t", value, ok)
	}
	if !cache.Put("c", "page-c", 4) {
		t.Fatal("Put(c) rejected an eligible page")
	}
	if _, ok := cache.Get("b"); ok {
		t.Fatal("least-recently-used page b remained resident")
	}
	if value, ok := cache.Get("a"); !ok || value != "page-a" {
		t.Fatalf("Get(a) after eviction = %q, %t", value, ok)
	}
	if value, ok := cache.Get("c"); !ok || value != "page-c" {
		t.Fatalf("Get(c) after eviction = %q, %t", value, ok)
	}
	stats := cache.Stats()
	if stats.Bytes != 8 || stats.Entries != 2 || stats.Hits != 3 || stats.Misses != 1 || stats.Evictions != 1 {
		t.Fatalf("Stats() = %#v", stats)
	}
}

func TestTT018PageIndexResidencyRejectsOversizedPagesAndReplacesInPlace(t *testing.T) {
	cache, err := hatDataStructure.NewPageIndexResidency[int, string](hatDataStructure.PageIndexResidencyOptions{MaxBytes: 4})
	if err != nil {
		t.Fatalf("NewPageIndexResidency: %v", err)
	}
	if cache.Put(1, "too-large", 5) {
		t.Fatal("oversized page was admitted")
	}
	if !cache.Put(1, "old", 2) || !cache.Put(1, "new", 3) {
		t.Fatal("eligible replacement was rejected")
	}
	if value, ok := cache.Get(1); !ok || value != "new" {
		t.Fatalf("Get(replaced) = %q, %t", value, ok)
	}
	stats := cache.Stats()
	if stats.Bytes != 3 || stats.Entries != 1 || stats.Replacements != 1 || stats.Rejections != 1 {
		t.Fatalf("Stats() = %#v", stats)
	}
	if !cache.Delete(1) || cache.Delete(1) {
		t.Fatal("Delete result did not describe residency")
	}
	if cache.Clear() != 0 || cache.Len() != 0 {
		t.Fatalf("Clear/Len after Delete = %d/%d", cache.Clear(), cache.Len())
	}
}

func TestTT018PageIndexResidencyValidatesOptionsAndNilReceiver(t *testing.T) {
	if _, err := hatDataStructure.NewPageIndexResidency[int, int](hatDataStructure.PageIndexResidencyOptions{}); !errors.Is(err, hatDataStructure.ErrPageIndexResidencyDisabled) {
		t.Fatalf("zero options error = %v", err)
	}
	if _, err := hatDataStructure.NewPageIndexResidency[int, int](hatDataStructure.PageIndexResidencyOptions{MaxBytes: 1, MaxEntries: -1}); !errors.Is(err, hatDataStructure.ErrPageIndexResidencyOptions) {
		t.Fatalf("negative MaxEntries error = %v", err)
	}
	var cache *hatDataStructure.PageIndexResidency[int, int]
	if value, ok := cache.Get(1); ok || value != 0 {
		t.Fatalf("nil Get = %d, %t", value, ok)
	}
	if cache.Put(1, 1, 1) || cache.Delete(1) || cache.Clear() != 0 || cache.Len() != 0 {
		t.Fatal("nil receiver mutated or reported residency")
	}
}

func TestTT018PageIndexResidencyGetIsAllocationFreeAndConcurrent(t *testing.T) {
	cache, err := hatDataStructure.NewPageIndexResidency[int, int](hatDataStructure.PageIndexResidencyOptions{MaxBytes: 1024})
	if err != nil {
		t.Fatalf("NewPageIndexResidency: %v", err)
	}
	if !cache.Put(1, 42, 1) {
		t.Fatal("Put(1) rejected")
	}
	if allocations := testing.AllocsPerRun(100, func() {
		if value, ok := cache.Get(1); !ok || value != 42 {
			t.Fatal("Get(1) lost resident page")
		}
	}); allocations != 0 {
		t.Fatalf("Get allocations = %v, want 0", allocations)
	}
	var wait sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			for index := 0; index < 100; index++ {
				key := worker*100 + index
				cache.Put(key, key, 1)
				cache.Get(key)
			}
		}(worker)
	}
	wait.Wait()
}

func TestTT018PageIndexResidencyReusesEvictedSlots(t *testing.T) {
	cache, err := hatDataStructure.NewPageIndexResidency[int, int](hatDataStructure.PageIndexResidencyOptions{
		MaxBytes:   2,
		MaxEntries: 2,
	})
	if err != nil {
		t.Fatalf("NewPageIndexResidency: %v", err)
	}
	for key := 0; key < 100; key++ {
		if !cache.Put(key, key*10, 1) {
			t.Fatalf("Put(%d) rejected", key)
		}
	}
	if value, ok := cache.Get(99); !ok || value != 990 {
		t.Fatalf("Get(99) = %d, %t", value, ok)
	}
	if value, ok := cache.Get(98); !ok || value != 980 {
		t.Fatalf("Get(98) = %d, %t", value, ok)
	}
	if _, ok := cache.Get(97); ok {
		t.Fatal("old evicted page remained resident")
	}
	stats := cache.Stats()
	if stats.Entries != 2 || stats.Bytes != 2 || stats.Evictions != 98 {
		t.Fatalf("Stats() = %#v", stats)
	}
}
