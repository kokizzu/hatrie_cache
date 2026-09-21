package hatReplication_test

import "testing"

type tt003BaselineRoute struct {
	nodeID  string
	healthy bool
}

func tt003BaselineRouteHash(key string) uint64 {
	const offset = uint64(14695981039346656037)
	const prime = uint64(1099511628211)
	hash := offset
	for index := 0; index < len(key); index++ {
		hash ^= uint64(key[index])
		hash *= prime
	}
	return hash
}

func tt003BaselineSelect(key string, routes []tt003BaselineRoute) (tt003BaselineRoute, bool) {
	if len(routes) == 0 {
		return tt003BaselineRoute{}, false
	}
	start := int(tt003BaselineRouteHash(key) % uint64(len(routes)))
	for offset := 0; offset < len(routes); offset++ {
		route := routes[(start+offset)%len(routes)]
		if route.healthy {
			return route, true
		}
	}
	return tt003BaselineRoute{}, false
}

var tt003BaselineRouteSink tt003BaselineRoute

// BenchmarkTT003ExistingRouteScan measures a caller-side health-aware scan
// before a reusable generation-fenced route cache exists.
func BenchmarkTT003ExistingRouteScan(b *testing.B) {
	routes := make([]tt003BaselineRoute, 16)
	for index := range routes {
		routes[index] = tt003BaselineRoute{nodeID: "node-" + string(rune('a'+index)), healthy: true}
	}
	keys := []string{"tenant-a", "tenant-b", "tenant-c", "tenant-d"}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		route, ok := tt003BaselineSelect(keys[index%len(keys)], routes)
		if !ok {
			b.Fatal("baseline route selection returned no route")
		}
		tt003BaselineRouteSink = route
	}
}
