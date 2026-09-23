package hatSql

import "testing"

var chG04RuntimeBloomSink []int

func BenchmarkCHG04HashIndexNumericMissHeavy(b *testing.B) {
	index := newSQLJoinHashIndex(4096)
	for row := 0; row < 4096; row++ {
		index.Add(float64(row), row)
	}
	probes := make([]float64, 4096)
	for index := range probes {
		probes[index] = float64(1_000_000 + index)
	}
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		chG04RuntimeBloomSink = index.Lookup(probes[iteration%len(probes)])
	}
}

func BenchmarkCHG04HashIndexNumericHitHeavy(b *testing.B) {
	index := newSQLJoinHashIndex(4096)
	for row := 0; row < 4096; row++ {
		index.Add(float64(row), row)
	}
	probes := make([]float64, 4096)
	for index := range probes {
		probes[index] = float64(index)
	}
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		chG04RuntimeBloomSink = index.Lookup(probes[iteration%len(probes)])
	}
}

func BenchmarkCHG04HashIndexNumericMissHeavyBaseline(b *testing.B) {
	index := newSQLJoinHashIndex(4096)
	index.bloomState = sqlJoinRuntimeBloomDisabled
	for row := 0; row < 4096; row++ {
		index.Add(float64(row), row)
	}
	probes := make([]float64, 4096)
	for index := range probes {
		probes[index] = float64(1_000_000 + index)
	}
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		chG04RuntimeBloomSink = index.Lookup(probes[iteration%len(probes)])
	}
}

func BenchmarkCHG04HashIndexNumericHitHeavyBaseline(b *testing.B) {
	index := newSQLJoinHashIndex(4096)
	index.bloomState = sqlJoinRuntimeBloomDisabled
	for row := 0; row < 4096; row++ {
		index.Add(float64(row), row)
	}
	probes := make([]float64, 4096)
	for index := range probes {
		probes[index] = float64(index)
	}
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		chG04RuntimeBloomSink = index.Lookup(probes[iteration%len(probes)])
	}
}
