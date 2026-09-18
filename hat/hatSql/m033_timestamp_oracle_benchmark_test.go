//go:build !m033baseline

package hatSql

import "testing"

func BenchmarkM033TimestampOracleReserve(b *testing.B) {
	const batchSize = 1024
	oracle := NewSQLLogicalTimestampOracle(0)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if _, err := oracle.Reserve(batchSize); err != nil {
			b.Fatal(err)
		}
	}
}
