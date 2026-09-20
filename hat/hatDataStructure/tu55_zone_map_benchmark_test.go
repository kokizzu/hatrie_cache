package hatDataStructure

import "testing"

const benchmarkTU55Rows = 1 << 20

var benchmarkTU55Sink int

type benchmarkTU55FullBound struct {
	min   uint64
	max   uint64
	start uint64
	end   uint64
}

func benchmarkTU55ClusteredValues() []uint64 {
	values := make([]uint64, benchmarkTU55Rows)
	for index := range values {
		values[index] = uint64(index / 64)
	}
	return values
}

func benchmarkTU55ScatteredValues() []uint64 {
	values := make([]uint64, benchmarkTU55Rows)
	for index := range values {
		values[index] = uint64((index * 2654435761) % benchmarkTU55Rows)
	}
	return values
}

func benchmarkTU55ZoneMap() (*ZoneMapIndex[uint64], []uint64) {
	values := benchmarkTU55ClusteredValues()
	index, err := NewZoneMapIndex(64, func(left, right uint64) bool {
		return left < right
	})
	if err != nil {
		panic(err)
	}
	if err := index.Build(values); err != nil {
		panic(err)
	}
	return index, values
}

func BenchmarkTU55FullValuesBuild1M(b *testing.B) {
	values := benchmarkTU55ClusteredValues()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		copyOfValues := append([]uint64(nil), values...)
		benchmarkTU55Sink = int(copyOfValues[len(copyOfValues)-1])
	}
}

func BenchmarkTU55ZoneMapBuild1M(b *testing.B) {
	values := benchmarkTU55ClusteredValues()
	index, err := NewZoneMapIndex(64, func(left, right uint64) bool {
		return left < right
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if err := index.Build(values); err != nil {
			b.Fatal(err)
		}
		benchmarkTU55Sink = index.SegmentCount()
	}
}

func BenchmarkTU55FullBoundsBuild1M(b *testing.B) {
	values := benchmarkTU55ClusteredValues()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		bounds := make([]benchmarkTU55FullBound, len(values))
		for index, value := range values {
			bounds[index] = benchmarkTU55FullBound{
				min:   value,
				max:   value,
				start: uint64(index),
				end:   uint64(index + 1),
			}
		}
		benchmarkTU55Sink = int(bounds[len(bounds)-1].max)
	}
}

func BenchmarkTU55FullScanEquality1M(b *testing.B) {
	values := benchmarkTU55ClusteredValues()
	query := uint64(8000)
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		matches := 0
		for _, value := range values {
			if value == query {
				matches++
			}
		}
		benchmarkTU55Sink = matches
	}
}

func BenchmarkTU55ZoneMapEquality1M(b *testing.B) {
	index, values := benchmarkTU55ZoneMap()
	query := uint64(8000)
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		matches := 0
		index.VisitEqual(query, func(segment ZoneMapSegment[uint64]) bool {
			for _, value := range values[segment.Start:segment.End] {
				if value == query {
					matches++
				}
			}
			return true
		})
		benchmarkTU55Sink = matches
	}
}

func BenchmarkTU55FullScanEqualityScattered1M(b *testing.B) {
	values := benchmarkTU55ScatteredValues()
	query := uint64(8000)
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		matches := 0
		for _, value := range values {
			if value == query {
				matches++
			}
		}
		benchmarkTU55Sink = matches
	}
}

func BenchmarkTU55ZoneMapEqualityScattered1M(b *testing.B) {
	values := benchmarkTU55ScatteredValues()
	index, err := NewZoneMapIndex(64, func(left, right uint64) bool {
		return left < right
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := index.Build(values); err != nil {
		b.Fatal(err)
	}
	query := uint64(8000)
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		matches := 0
		index.VisitEqual(query, func(segment ZoneMapSegment[uint64]) bool {
			for _, value := range values[segment.Start:segment.End] {
				if value == query {
					matches++
				}
			}
			return true
		})
		benchmarkTU55Sink = matches
	}
}
