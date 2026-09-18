//go:build m033baseline

package hatSql

import (
	"sync/atomic"
	"testing"
)

func BenchmarkM033TimestampOracleBaseline(b *testing.B) {
	const batchSize = 1024
	var timestamp atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		for index := 0; index < batchSize; index++ {
			timestamp.Add(1)
		}
	}
}
