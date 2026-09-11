package hatSql

import "testing"

func BenchmarkSQLGroupRowsWithKeyLimit(b *testing.B) {
	rows, by, query := maxGroupKeysBenchmarkInput()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		groups, err := groupSQLRowsWithLimit(rows, by, query, 256)
		if err != nil {
			b.Fatal(err)
		}
		maxGroupKeysBenchmarkSink = groups
	}
}
