package hatReplication_test

import (
	"testing"
	"time"

	hatReplication "hatrie_cache/hat/hatReplication"
)

var tt003RouteCacheSink hatReplication.FailoverRoute

// BenchmarkTT003RouteCacheLookup measures the lock-free lookup path against
// the existing caller-side scan benchmark.
func BenchmarkTT003RouteCacheLookup(b *testing.B) {
	cache, err := hatReplication.NewFailoverRouteCache(hatReplication.FailoverRouteCacheOptions{MaxRoutes: 16})
	if err != nil {
		b.Fatal(err)
	}
	routes := make([]hatReplication.FailoverRoute, 16)
	for index := range routes {
		routes[index] = hatReplication.FailoverRoute{NodeID: "node-" + string(rune('a'+index)), Address: "node.internal:9000"}
	}
	if err := cache.Replace(1, routes); err != nil {
		b.Fatal(err)
	}
	keys := []string{"tenant-a", "tenant-b", "tenant-c", "tenant-d"}
	now := time.Unix(100, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		route, err := cache.Lookup(keys[index%len(keys)], now)
		if err != nil {
			b.Fatal(err)
		}
		tt003RouteCacheSink = route
	}
}
