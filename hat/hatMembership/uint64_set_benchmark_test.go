package hatMembership

import "testing"

func BenchmarkLinearContainsSmall(b *testing.B) {
	values := make([]uint64, 32)
	for i := range values {
		values[i] = uint64(i * 3)
	}
	var found int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if linearContains(values, uint64(i%96)) {
			found++
		}
	}
	b.StopTimer()
	if found == -1 {
		b.Fatal(found)
	}
}

func BenchmarkAdaptiveSortedContains(b *testing.B) {
	values := make([]uint64, 32)
	for i := range values {
		values[i] = uint64(i * 3)
	}
	set := BuildUint64(values)
	benchmarkContains(b, set)
}

func BenchmarkAdaptiveBitmapContains(b *testing.B) {
	values := make([]uint64, 1000)
	for i := range values {
		values[i] = uint64(i * 2)
	}
	set := BuildUint64(values)
	benchmarkContains(b, set)
}

func BenchmarkAdaptiveHashContains(b *testing.B) {
	values := make([]uint64, 512)
	for i := range values {
		values[i] = uint64(i) * 1_000_000_000
	}
	set := BuildUint64(values)
	benchmarkContains(b, set)
}

func BenchmarkBuildDenseBitmap(b *testing.B) {
	values := make([]uint64, 1000)
	for i := range values {
		values[i] = uint64(i * 2)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if BuildUint64(values).Mode() != ModeBitmap {
			b.Fatal("unexpected representation")
		}
	}
}

func BenchmarkBuildSparseHash(b *testing.B) {
	values := make([]uint64, 512)
	for i := range values {
		values[i] = uint64(i) * 1_000_000_000
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if BuildUint64(values).Mode() != ModeHash {
			b.Fatal("unexpected representation")
		}
	}
}

func benchmarkContains(b *testing.B, set *Uint64Set) {
	var found int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if set.Contains(uint64(i % 2048)) {
			found++
		}
	}
	b.StopTimer()
	if found == -1 {
		b.Fatal(found)
	}
}

func linearContains(values []uint64, needle uint64) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}
