package hatPipeline

import "testing"

var mz008FrontierAntichainCoverSink bool

func BenchmarkMZ008FrontierAntichainInsert(b *testing.B) {
	points := mz008BenchmarkPoints()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		antichain, err := NewFrontierAntichain(4, FrontierAntichainOptions{MaxPoints: len(points) + 1})
		if err != nil {
			b.Fatal(err)
		}
		for _, point := range points {
			if _, err := antichain.Insert(point); err != nil {
				b.Fatal(err)
			}
		}
		if antichain.Len() != len(points) {
			b.Fatalf("Len() = %d, want %d", antichain.Len(), len(points))
		}
	}
}

func BenchmarkMZ008FrontierAntichainInsertPresized(b *testing.B) {
	points := mz008BenchmarkPoints()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		antichain, err := NewFrontierAntichain(4, FrontierAntichainOptions{MaxPoints: len(points) + 1, InitialPoints: len(points)})
		if err != nil {
			b.Fatal(err)
		}
		for _, point := range points {
			if _, err := antichain.Insert(point); err != nil {
				b.Fatal(err)
			}
		}
		if antichain.Len() != len(points) {
			b.Fatalf("presized Len() = %d, want %d", antichain.Len(), len(points))
		}
	}
}

func BenchmarkMZ008NestedSliceAntichainInsert(b *testing.B) {
	points := mz008BenchmarkPoints()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		frontier := make([][]uint64, 0, len(points))
		for _, point := range points {
			dominated := false
			for _, existing := range frontier {
				if frontierAntichainLessEqual(existing, point) {
					dominated = true
					break
				}
			}
			if dominated {
				continue
			}
			kept := frontier[:0]
			for _, existing := range frontier {
				if !frontierAntichainLessEqual(point, existing) {
					kept = append(kept, existing)
				}
			}
			frontier = append(kept, append([]uint64(nil), point...))
		}
		if len(frontier) != len(points) {
			b.Fatalf("nested antichain length = %d, want %d", len(frontier), len(points))
		}
	}
}

func BenchmarkMZ008FrontierAntichainCovers(b *testing.B) {
	points := mz008BenchmarkPoints()
	antichain, err := NewFrontierAntichain(4, FrontierAntichainOptions{MaxPoints: len(points) + 1})
	if err != nil {
		b.Fatal(err)
	}
	for _, point := range points {
		if _, err := antichain.Insert(point); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		covered, err := antichain.Covers([]uint64{uint64(i % len(points)), uint64(len(points)), 0, 0})
		if err != nil {
			b.Fatal(err)
		}
		mz008FrontierAntichainCoverSink = covered
	}
}

func BenchmarkMZ008FrontierAntichainSnapshot(b *testing.B) {
	points := mz008BenchmarkPoints()
	antichain, err := NewFrontierAntichain(4, FrontierAntichainOptions{MaxPoints: len(points) + 1})
	if err != nil {
		b.Fatal(err)
	}
	for _, point := range points {
		if _, err := antichain.Insert(point); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snapshot := antichain.Snapshot()
		if snapshot.Len() != len(points) {
			b.Fatalf("Snapshot().Len() = %d, want %d", snapshot.Len(), len(points))
		}
	}
}

func mz008BenchmarkPoints() [][]uint64 {
	points := make([][]uint64, 256)
	for index := range points {
		points[index] = []uint64{uint64(index), uint64(len(points) - index), 0, 0}
	}
	return points
}
