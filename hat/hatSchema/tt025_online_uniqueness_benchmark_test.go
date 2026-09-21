package hatSchema

import (
	"strconv"
	"testing"
)

func newTT025UniqueBenchmarkSource(b *testing.B) *MaterializedSource {
	b.Helper()
	source := NewMaterializedSource([]DerivedColumn{{Name: "id"}, {Name: "email"}})
	for index := 0; index < 10_000; index++ {
		if _, err := source.Insert(Row{
			"id":    int64(index),
			"email": "email-" + strconv.Itoa(index),
		}); err != nil {
			b.Fatal(err)
		}
	}
	return source
}

func BenchmarkTT025SecondaryIndexBuildUniqueFixture(b *testing.B) {
	source := newTT025UniqueBenchmarkSource(b)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		report, err := source.BuildSecondaryIndex("email")
		if err != nil || report.Rows != 10_000 {
			b.Fatalf("BuildSecondaryIndex() report/error = %#v/%v", report, err)
		}
	}
}

func BenchmarkTT025UniqueIndexBuild(b *testing.B) {
	source := newTT025UniqueBenchmarkSource(b)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		report, err := source.BuildUniqueIndex("email")
		if err != nil || report.Rows != 10_000 {
			b.Fatalf("BuildUniqueIndex() report/error = %#v/%v", report, err)
		}
	}
}

func BenchmarkTT025RegularIndexedInsert(b *testing.B) {
	benchmarkTT025IndexedInsert(b, false)
}

func BenchmarkTT025UniqueIndexedInsert(b *testing.B) {
	benchmarkTT025IndexedInsert(b, true)
}

func benchmarkTT025IndexedInsert(b *testing.B, unique bool) {
	const seedRows = 1_024
	newSource := func() *MaterializedSource {
		columns := []DerivedColumn{{Name: "id"}, {Name: "email", Indexed: !unique}}
		source := NewMaterializedSource(columns)
		for index := 0; index < seedRows; index++ {
			if _, err := source.Insert(Row{"id": int64(index), "email": "seed-" + strconv.Itoa(index)}); err != nil {
				b.Fatal(err)
			}
		}
		if unique {
			if _, err := source.BuildUniqueIndex("email"); err != nil {
				b.Fatal(err)
			}
		}
		return source
	}

	source := newSource()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if index > 0 && index%seedRows == 0 {
			b.StopTimer()
			source = newSource()
			b.StartTimer()
		}
		if _, err := source.Insert(Row{
			"id":    int64(seedRows + index),
			"email": "new-" + strconv.Itoa(index),
		}); err != nil {
			b.Fatal(err)
		}
	}
}
