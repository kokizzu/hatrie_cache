package hatDataStructure

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestTU18VolatileCacheValidation(t *testing.T) {
	if _, err := NewVolatileCache[string, int](VolatileCacheOptions[string, int]{}); !errors.Is(err, ErrVolatileCacheInvalidCapacity) {
		t.Fatalf("expected invalid capacity, got %v", err)
	}
	if _, err := NewVolatileCache[string, int](VolatileCacheOptions[string, int]{Capacity: 1, MaxBytes: -1}); !errors.Is(err, ErrVolatileCacheInvalidMaxBytes) {
		t.Fatalf("expected invalid max bytes, got %v", err)
	}
	if _, err := NewVolatileCache[string, int](VolatileCacheOptions[string, int]{Capacity: 1, MaxBytes: 1}); !errors.Is(err, ErrVolatileCacheMissingSizer) {
		t.Fatalf("expected missing sizer, got %v", err)
	}

	cache, err := NewVolatileCache[string, int](VolatileCacheOptions[string, int]{Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Set("key", 1, -time.Second); !errors.Is(err, ErrVolatileCacheNegativeTTL) {
		t.Fatalf("expected negative ttl error, got %v", err)
	}
	negativeSizer, err := NewVolatileCache[string, int](VolatileCacheOptions[string, int]{
		Capacity: 1,
		SizeOf:   func(string, int) int { return -1 },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := negativeSizer.Set("key", 1, 0); !errors.Is(err, ErrVolatileCacheNegativeItemSize) {
		t.Fatalf("expected negative item size error, got %v", err)
	}
}

func TestTU18VolatileCacheLRUAndStats(t *testing.T) {
	cache, err := NewVolatileCache[string, int](VolatileCacheOptions[string, int]{
		Capacity: 2,
		SizeOf:   func(string, int) int { return 8 },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Set("a", 1, 0); err != nil {
		t.Fatal(err)
	}
	if err := cache.Set("b", 2, 0); err != nil {
		t.Fatal(err)
	}
	if value, ok := cache.Get("a"); !ok || value != 1 {
		t.Fatalf("get a = %v, %v", value, ok)
	}
	if err := cache.Set("c", 3, 0); err != nil {
		t.Fatal(err)
	}
	if _, ok := cache.Get("b"); ok {
		t.Fatal("least recently used b was not evicted")
	}
	if value, ok := cache.Peek("a"); !ok || value != 1 {
		t.Fatalf("peek a = %v, %v", value, ok)
	}
	if cache.Len() != 2 {
		t.Fatalf("len = %d, want 2", cache.Len())
	}
	stats := cache.Stats()
	if stats.Sets != 3 || stats.Evictions != 1 || stats.Hits != 2 || stats.Misses != 1 || stats.Items != 2 || stats.Bytes != 16 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestTU18VolatileCacheTTLAndPurge(t *testing.T) {
	now := time.Unix(100, 0)
	cache, err := NewVolatileCache[string, int](VolatileCacheOptions[string, int]{
		Capacity: 4,
		Now:      func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Set("persistent", 1, 0); err != nil {
		t.Fatal(err)
	}
	if err := cache.Set("short", 2, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	now = now.Add(4 * time.Second)
	if _, ok := cache.Get("short"); !ok {
		t.Fatal("short entry expired too early")
	}
	now = now.Add(time.Second)
	if _, ok := cache.Get("short"); ok {
		t.Fatal("short entry did not expire")
	}
	if removed := cache.PurgeExpired(now); removed != 0 {
		t.Fatalf("purged %d entries after lazy expiry", removed)
	}
	if _, ok := cache.Get("persistent"); !ok {
		t.Fatal("zero ttl entry expired")
	}
	if err := cache.Set("long", 3, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	now = now.Add(10 * time.Second)
	if removed := cache.PurgeExpired(now); removed != 1 {
		t.Fatalf("purged %d entries, want 1", removed)
	}
	stats := cache.Stats()
	if stats.Expirations != 2 || stats.Items != 1 {
		t.Fatalf("unexpected ttl stats: %+v", stats)
	}
}

func TestTU18VolatileCacheByteLimitAndUpdate(t *testing.T) {
	cache, err := NewVolatileCache[string, int](VolatileCacheOptions[string, int]{
		Capacity: 3,
		MaxBytes: 10,
		SizeOf:   func(_ string, value int) int { return value },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Set("a", 4, 0); err != nil {
		t.Fatal(err)
	}
	if err := cache.Set("a", 6, 0); err != nil {
		t.Fatal(err)
	}
	if err := cache.Set("a", 11, 0); !errors.Is(err, ErrVolatileCacheItemTooLarge) {
		t.Fatalf("expected oversized item error, got %v", err)
	}
	if value, ok := cache.Get("a"); !ok || value != 6 {
		t.Fatalf("oversized update changed a: %v, %v", value, ok)
	}
	if err := cache.Set("b", 4, 0); err != nil {
		t.Fatal(err)
	}
	if cache.Len() != 2 || cache.Stats().Bytes != 10 {
		t.Fatalf("unexpected size state: len=%d stats=%+v", cache.Len(), cache.Stats())
	}
	if err := cache.Set("c", 1, 0); err != nil {
		t.Fatal(err)
	}
	if cache.Len() != 2 || cache.Stats().Bytes != 5 {
		t.Fatalf("byte eviction state: len=%d stats=%+v", cache.Len(), cache.Stats())
	}
}

func TestTU18VolatileCacheDeleteClearAndNil(t *testing.T) {
	var nilCache *VolatileCache[string, int]
	if _, ok := nilCache.Get("missing"); ok {
		t.Fatal("nil cache returned a value")
	}
	if err := nilCache.Set("key", 1, 0); !errors.Is(err, ErrVolatileCacheNil) {
		t.Fatalf("nil set error = %v", err)
	}
	if nilCache.Len() != 0 || nilCache.Stats() != (VolatileCacheStats{}) {
		t.Fatalf("nil cache state: len=%d stats=%+v", nilCache.Len(), nilCache.Stats())
	}

	cache, err := NewVolatileCache[string, int](VolatileCacheOptions[string, int]{Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Set("key", 1, 0); err != nil {
		t.Fatal(err)
	}
	if !cache.Delete("key") || cache.Delete("key") {
		t.Fatal("delete result mismatch")
	}
	if err := cache.Set("a", 1, 0); err != nil {
		t.Fatal(err)
	}
	if err := cache.Set("b", 2, 0); err != nil {
		t.Fatal(err)
	}
	cache.Clear()
	if cache.Len() != 0 || cache.Stats().Items != 0 {
		t.Fatalf("clear state: len=%d stats=%+v", cache.Len(), cache.Stats())
	}
}

func TestTU18VolatileCacheUpdateEvictsForByteLimit(t *testing.T) {
	cache, err := NewVolatileCache[string, int](VolatileCacheOptions[string, int]{
		Capacity: 3,
		MaxBytes: 10,
		SizeOf:   func(_ string, value int) int { return value },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Set("a", 4, 0); err != nil {
		t.Fatal(err)
	}
	if err := cache.Set("b", 6, 0); err != nil {
		t.Fatal(err)
	}
	if err := cache.Set("a", 8, 0); err != nil {
		t.Fatal(err)
	}
	if _, ok := cache.Get("b"); ok {
		t.Fatal("byte-limit update did not evict the older entry")
	}
	if value, ok := cache.Get("a"); !ok || value != 8 {
		t.Fatalf("updated value = %v, %v", value, ok)
	}
	if stats := cache.Stats(); stats.Bytes != 8 || stats.Evictions != 1 {
		t.Fatalf("unexpected update stats: %+v", stats)
	}
}

func TestTU18VolatileCacheConcurrentAccess(t *testing.T) {
	cache, err := NewVolatileCache[int, int](VolatileCacheOptions[int, int]{Capacity: 64})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				key := (worker*1000 + i) % 128
				if err := cache.Set(key, i, 0); err != nil {
					t.Errorf("set: %v", err)
					return
				}
				cache.Get(key)
			}
		}(worker)
	}
	wg.Wait()
	if cache.Len() > 64 {
		t.Fatalf("len = %d, exceeds capacity", cache.Len())
	}
}
