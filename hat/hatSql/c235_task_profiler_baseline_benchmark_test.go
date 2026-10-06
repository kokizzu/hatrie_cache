package hatSql

import "testing"

type c235TaskProfilerMapKey struct {
	operation string
	table     string
	part      string
	column    string
}

type c235TaskProfilerMapTotals struct {
	operations uint64
	rows       uint64
	bytes      uint64
	elapsed    int64
}

var c235TaskProfilerMapBaselineSink c235TaskProfilerMapTotals

func BenchmarkC235TaskProfilerMapBaseline(b *testing.B) {
	profiles := make(map[c235TaskProfilerMapKey]c235TaskProfilerMapTotals, 8)
	key := c235TaskProfilerMapKey{operation: "read", table: "orders", part: "part-01", column: "customer_id"}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		profile := profiles[key]
		profile.operations++
		profile.rows += 10
		profile.bytes += 1200
		profile.elapsed += 40
		profiles[key] = profile
	}
	b.StopTimer()
	c235TaskProfilerMapBaselineSink = profiles[key]
}
