//go:build t205baseline

package hatReplication

import "testing"

func BenchmarkT205BaselineProgressArithmetic(b *testing.B) {
	var lag uint64
	var lsnRate uint64
	var byteRate uint64
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		sourceLSN := uint64(index) + 1_000_000
		appliedLSN := sourceLSN - 100
		appliedBytes := uint64(index) + 9_000_000
		previousLSN := appliedLSN - 10
		previousBytes := appliedBytes - 200
		lag += sourceLSN - appliedLSN
		lsnRate += appliedLSN - previousLSN
		byteRate += appliedBytes - previousBytes
	}
	if lag == 0 || lsnRate == 0 || byteRate == 0 {
		b.Fatal("benchmark did not calculate progress")
	}
}
