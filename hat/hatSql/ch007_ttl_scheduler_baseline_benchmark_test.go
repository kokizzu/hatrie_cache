package hatSql

import (
	"testing"
	"time"
)

func BenchmarkCH007TTLDirectPurgeBaseline(b *testing.B) {
	table := newCH007TTLBenchmarkTable(b, TypedTableTTLOptions{
		Mode:     TypedTableTTLProcessingTime,
		Lifetime: time.Hour,
		Clock:    func() time.Time { return time.Unix(100, 0) },
	})
	now := time.Unix(100, 0).Add(time.Minute)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		changes, err := table.PurgeExpired(now)
		if err != nil {
			b.Fatal(err)
		}
		ch007TTLBenchmarkSink += len(changes)
	}
}
