package hatPrimaryPruning

import "testing"

func BenchmarkLinearCompositePruning(b *testing.B) {
	marks := benchmarkMarks(10_000)
	query := benchmarkRange()
	var count int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, mark := range marks {
			if linearMayOverlap(mark, query) {
				count++
			}
		}
	}
	b.StopTimer()
	if count == -1 {
		b.Fatal(count)
	}
}

func BenchmarkPackedCompositePruning(b *testing.B) {
	index, err := Build(benchmarkMarks(10_000))
	if err != nil {
		b.Fatal(err)
	}
	query := benchmarkRange()
	dst := make([]int, 0, 10_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var candidates []int
		candidates, err = index.Candidates(query, dst[:0])
		if err != nil {
			b.Fatal(err)
		}
		if len(candidates) == 0 {
			b.Fatal("expected candidates")
		}
	}
}

func BenchmarkPackedMayOverlap(b *testing.B) {
	index, err := Build(benchmarkMarks(10_000))
	if err != nil {
		b.Fatal(err)
	}
	query := benchmarkRange()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !index.MayOverlap(5000, query) {
			b.Fatal("expected overlap")
		}
	}
}

func BenchmarkLinearCompositeFullRange(b *testing.B) {
	marks := benchmarkMarks(10_000)
	query := Range{}
	var count int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, mark := range marks {
			if linearMayOverlap(mark, query) {
				count++
			}
		}
	}
	b.StopTimer()
	if count == -1 {
		b.Fatal(count)
	}
}

func BenchmarkPackedCompositeFullRange(b *testing.B) {
	index, err := Build(benchmarkMarks(10_000))
	if err != nil {
		b.Fatal(err)
	}
	dst := make([]int, 0, 10_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var candidates []int
		candidates, err = index.Candidates(Range{}, dst[:0])
		if err != nil {
			b.Fatal(err)
		}
		if len(candidates) != 10_000 {
			b.Fatal("unexpected candidate count")
		}
	}
}

func BenchmarkBuildPackedMarks(b *testing.B) {
	marks := benchmarkMarks(10_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Build(marks); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkMarks(count int) []Mark {
	marks := make([]Mark, count)
	for i := range marks {
		base := uint64(i * 100)
		marks[i] = Mark{
			Min: []KeyPart{{Value: base}, {Value: 0}},
			Max: []KeyPart{{Value: base}, {Value: 99}},
		}
	}
	return marks
}

func benchmarkRange() Range {
	return Range{
		HasLower: true,
		Lower:    []KeyPart{{Value: 500_000}, {Value: 50}},
		HasUpper: true,
		Upper:    []KeyPart{{Value: 500_000}, {Value: 60}},
	}
}

func linearMayOverlap(mark Mark, query Range) bool {
	if query.HasLower && compareTuple(mark.Max, query.Lower, true) < 0 {
		return false
	}
	if query.HasUpper && compareTuple(mark.Min, query.Upper, true) > 0 {
		return false
	}
	return true
}
