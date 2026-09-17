package hatSql

import (
	"context"
	"sync"
	"testing"
)

type mz030LookupResolver struct {
	mu          sync.Mutex
	orders      []Row
	countries   map[string]string
	frontier    uint64
	ready       bool
	available   bool
	lookupCalls int
}

func (resolver *mz030LookupResolver) ResolveSQLSource(name, key string) ([]Row, error) {
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	if name == "CACHE" && key == "orders" {
		return resolver.orders, nil
	}
	return nil, nil
}

func (resolver *mz030LookupResolver) ResolveSQLLookupSource(name, key, field string, value interface{}) ([]Row, bool, error) {
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	resolver.lookupCalls++
	if name != "EXTERNAL" || key != "countries" || field != "code" {
		return nil, false, nil
	}
	if value == nil {
		return nil, true, nil
	}
	code, ok := value.(string)
	if !ok {
		return nil, true, nil
	}
	nameValue, ok := resolver.countries[code]
	if !ok {
		return nil, true, nil
	}
	return []Row{{"code": code, "name": nameValue}}, true, nil
}

func (resolver *mz030LookupResolver) ResolveSQLExternalSource(string) ([]Row, error) {
	return nil, nil
}

func (resolver *mz030LookupResolver) SQLSourceFrontier(name, key string) (uint64, bool, bool, error) {
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	if name != "EXTERNAL" || key != "countries" {
		return 0, false, false, nil
	}
	return resolver.frontier, resolver.ready, resolver.available, nil
}

func (resolver *mz030LookupResolver) calls() int {
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	return resolver.lookupCalls
}

func (resolver *mz030LookupResolver) setCountry(code, name string, frontier uint64) {
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	resolver.countries[code] = name
	resolver.frontier = frontier
}

func newMZ030LookupResolver() *mz030LookupResolver {
	return &mz030LookupResolver{
		orders: []Row{
			{"id": int64(1), "country": "SG"},
			{"id": int64(2), "country": "SG"},
			{"id": int64(3), "country": "JP"},
		},
		countries: map[string]string{"SG": "Singapore", "JP": "Japan"},
		frontier:  1,
		ready:     true,
		available: true,
	}
}

func runMZ030LookupJoin(t *testing.T, resolver *mz030LookupResolver, cache *SQLLookupJoinCache) SQLQueryResult {
	t.Helper()
	result, err := ExecuteQueryParameters(context.Background(), mz030LookupJoinQuery, resolver, nil, QueryOptions{LookupJoinCache: cache})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestMZ030LookupJoinCacheReusesStableFrontierAndInvalidates(t *testing.T) {
	resolver := newMZ030LookupResolver()
	cache := NewSQLLookupJoinCache(8)

	first := runMZ030LookupJoin(t, resolver, cache)
	if got, want := first.Rows[0]["name"], "Singapore"; got != want {
		t.Fatalf("first lookup name = %#v, want %q", got, want)
	}
	callsAfterFirst := resolver.calls()
	second := runMZ030LookupJoin(t, resolver, cache)
	if got, want := second.Rows[1]["name"], "Singapore"; got != want {
		t.Fatalf("cached lookup name = %#v, want %q", got, want)
	}
	if resolver.calls() != callsAfterFirst+1 {
		t.Fatalf("lookup calls after stable cache hit = %d, want probe-only increase from %d", resolver.calls(), callsAfterFirst)
	}
	if stats := cache.Stats(); stats.Hits < 2 {
		t.Fatalf("cache stats = %#v, want hits for repeated SG and JP lookups", stats)
	}

	resolver.setCountry("SG", "Singapore-updated", 2)
	third := runMZ030LookupJoin(t, resolver, cache)
	if got, want := third.Rows[0]["name"], "Singapore-updated"; got != want {
		t.Fatalf("invalidated lookup name = %#v, want %q", got, want)
	}
	if stats := cache.Stats(); stats.Invalidations == 0 {
		t.Fatalf("cache stats = %#v, want a frontier invalidation", stats)
	}
}

func TestMZ030LookupJoinCacheBypassesUnavailableFrontier(t *testing.T) {
	resolver := newMZ030LookupResolver()
	cache := NewSQLLookupJoinCache(8)
	resolver.available = false
	result := runMZ030LookupJoin(t, resolver, cache)
	if got, want := result.Rows[0]["name"], "Singapore"; got != want {
		t.Fatalf("fallback lookup name = %#v, want %q", got, want)
	}
	if stats := cache.Stats(); stats.Bypasses == 0 {
		t.Fatalf("cache stats = %#v, want unavailable-frontier bypass", stats)
	}
}

func TestMZ030LookupJoinCacheClearAndBounds(t *testing.T) {
	resolver := newMZ030LookupResolver()
	cache, err := NewSQLLookupJoinCacheWithOptions(SQLLookupJoinCacheOptions{
		Capacity:         1,
		MaxRowsPerEntry:  1,
		MaxBytesPerEntry: 1 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = runMZ030LookupJoin(t, resolver, cache)
	firstCalls := resolver.calls()
	cache.Clear()
	_ = runMZ030LookupJoin(t, resolver, cache)
	if resolver.calls() <= firstCalls+1 {
		t.Fatalf("lookup calls after Clear = %d, want cache misses after probe", resolver.calls())
	}
	if stats := cache.Stats(); stats.Entries == 0 {
		t.Fatalf("cache stats = %#v, want a retained entry after clear and refill", stats)
	}

	if _, err := NewSQLLookupJoinCacheWithOptions(SQLLookupJoinCacheOptions{Capacity: -1}); err != ErrSQLLookupJoinCacheCapacityInvalid {
		t.Fatalf("negative capacity error = %v, want %v", err, ErrSQLLookupJoinCacheCapacityInvalid)
	}
	if _, err := NewSQLLookupJoinCacheWithOptions(SQLLookupJoinCacheOptions{MaxRowsPerEntry: -1}); err != ErrSQLLookupJoinCacheRowsInvalid {
		t.Fatalf("negative row bound error = %v, want %v", err, ErrSQLLookupJoinCacheRowsInvalid)
	}
	if _, err := NewSQLLookupJoinCacheWithOptions(SQLLookupJoinCacheOptions{MaxBytesPerEntry: -1}); err != ErrSQLLookupJoinCacheBytesInvalid {
		t.Fatalf("negative byte bound error = %v, want %v", err, ErrSQLLookupJoinCacheBytesInvalid)
	}
}
