package hatPipeline

import "testing"

var lateDataReclockBenchmarkSink uint64

func BenchmarkMZ032LinearSourceLookup(b *testing.B) {
	bindings := benchmarkMZ032Bindings(b)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		lateDataReclockBenchmarkSink = lateDataReclockLinearSourceLookup(bindings, uint64(iteration%4095))
	}
}

func BenchmarkMZ032BinarySourceLookup(b *testing.B) {
	reclock := benchmarkMZ032Reclock(b)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		value, err := reclock.ProcessingFrontierAt(uint64(iteration % 4095))
		if err != nil {
			b.Fatal(err)
		}
		lateDataReclockBenchmarkSink = value
	}
}

func BenchmarkMZ032LinearProcessingLookup(b *testing.B) {
	bindings := benchmarkMZ032Bindings(b)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		lateDataReclockBenchmarkSink = uint64(lateDataReclockLinearProcessingLookup(bindings, uint64(iteration%4096)))
	}
}

func BenchmarkMZ032BinaryProcessingLookup(b *testing.B) {
	reclock := benchmarkMZ032Reclock(b)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		value, err := reclock.SourceFrontierAt(uint64(iteration % 4096))
		if err != nil {
			b.Fatal(err)
		}
		lateDataReclockBenchmarkSink = value
	}
}

func benchmarkMZ032Bindings(b *testing.B) []LateDataReclockBinding {
	b.Helper()
	bindings := make([]LateDataReclockBinding, 4096)
	for index := range bindings {
		bindings[index] = LateDataReclockBinding{
			ProcessingFrontier: uint64(index),
			SourceFrontier:     uint64(index),
		}
	}
	return bindings
}

func benchmarkMZ032Reclock(b *testing.B) *LateDataReclock {
	b.Helper()
	reclock, err := NewLateDataReclock(LateDataReclockOptions{MaxBindings: 4096})
	if err != nil {
		b.Fatal(err)
	}
	for index := 1; index < 4096; index++ {
		if _, err := reclock.ObserveSourceFrontier(uint64(index)); err != nil {
			b.Fatal(err)
		}
		if _, _, err := reclock.AdvanceProcessingFrontier(uint64(index)); err != nil {
			b.Fatal(err)
		}
	}
	return reclock
}

func lateDataReclockLinearSourceLookup(bindings []LateDataReclockBinding, source uint64) uint64 {
	for _, binding := range bindings {
		if binding.SourceFrontier > source {
			return binding.ProcessingFrontier
		}
	}
	return 0
}

func lateDataReclockLinearProcessingLookup(bindings []LateDataReclockBinding, processing uint64) int {
	result := -1
	for index, binding := range bindings {
		if binding.ProcessingFrontier > processing {
			break
		}
		result = index
	}
	if result < 0 {
		return 0
	}
	return int(bindings[result].SourceFrontier)
}
