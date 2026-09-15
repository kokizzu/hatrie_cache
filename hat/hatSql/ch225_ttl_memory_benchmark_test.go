package hatSql

import (
	"testing"
	"time"
	"unsafe"
)

func BenchmarkCH225TypedTableExpiryIndexMemory(b *testing.B) {
	table := newCH007TTLBenchmarkTable(b, TypedTableTTLOptions{
		Mode:     TypedTableTTLProcessingTime,
		Lifetime: time.Hour,
		Clock:    func() time.Time { return time.Unix(100, 0) },
	})
	b.ReportMetric(float64(len(table.ttl.deadlines)*int(unsafe.Sizeof(int64(0)))), "ttl_deadline_bytes")
	b.ReportMetric(float64(len(table.ttl.expiryHeap)*int(unsafe.Sizeof(typedTableTTLExpiryEntry{}))+len(table.ttl.expiryPositions)*int(unsafe.Sizeof(int(0)))), "ttl_expiry_index_bytes")
	b.ReportMetric(float64(len(table.keys)), "ttl_rows")
	for index := 0; index < b.N; index++ {
		ch007TTLBenchmarkSink += len(table.ttl.expiryHeap)
	}
}
