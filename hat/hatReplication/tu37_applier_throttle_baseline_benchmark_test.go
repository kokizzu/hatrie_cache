//go:build tu37baseline

package hatReplication

import "testing"

func BenchmarkTU37UnthrottledApplyAdmission(b *testing.B) {
	const batchSize = 128
	var applied uint64
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		applied += batchSize
	}
	if applied == 0 {
		b.Fatal("benchmark did not apply a batch")
	}
}
