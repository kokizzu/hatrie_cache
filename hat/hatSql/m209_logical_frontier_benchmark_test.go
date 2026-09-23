package hatSql

import "testing"

func BenchmarkM209ReadFrontierDefaultPath(b *testing.B) {
	options := SQLQueryOptions{}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := options.normalizeSQLSnapshotToken(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM209ReadFrontierGuardedPath(b *testing.B) {
	frontier := NewSQLLogicalFrontier(5)
	asOf := uint64(5)
	options := SQLQueryOptions{AsOfFrontier: &asOf, LogicalFrontier: frontier}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := options.normalizeSQLSnapshotToken(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM209LogicalFrontierAdvance(b *testing.B) {
	frontier := NewSQLLogicalFrontier(1)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := frontier.Advance(1); err != nil {
			b.Fatal(err)
		}
	}
}
