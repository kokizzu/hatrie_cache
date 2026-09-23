package hatSql

import "testing"

func BenchmarkM211AsOfBoundsDefaultPath(b *testing.B) {
	options := SQLQueryOptions{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := options.normalizeSQLSnapshotToken(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM211AsOfBoundsGuardedPath(b *testing.B) {
	frontier := uint64(10)
	since := uint64(5)
	upper := uint64(20)
	options := SQLQueryOptions{AsOfFrontier: &frontier, AsOfSince: &since, AsOfUpper: &upper}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := options.normalizeSQLSnapshotToken(); err != nil {
			b.Fatal(err)
		}
	}
}
