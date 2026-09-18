package hatSchema

import "testing"

func BenchmarkTT021OnlineSecondaryIndexBuild(b *testing.B) {
	source := benchmarkTT021MaterializedSource(b)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		report, err := source.BuildSecondaryIndex("region")
		if err != nil || report.Rows != 10_000 {
			b.Fatalf("BuildSecondaryIndex() report/error = %#v/%v", report, err)
		}
	}
}
