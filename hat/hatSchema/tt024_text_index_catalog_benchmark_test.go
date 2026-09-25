package hatSchema

import (
	"context"
	"strconv"
	"testing"
)

type tt024TextIndexCatalogBenchmarkMemory struct {
	frame []byte
}

func (catalog *tt024TextIndexCatalogBenchmarkMemory) PutTextIndex(_ context.Context, _, _ string, frame []byte) error {
	catalog.frame = append(catalog.frame[:0], frame...)
	return nil
}

func (catalog *tt024TextIndexCatalogBenchmarkMemory) GetTextIndex(context.Context, string, string) ([]byte, error) {
	return append([]byte(nil), catalog.frame...), nil
}

func benchmarkTT024TextIndexCatalogSource(b *testing.B) *MaterializedSource {
	b.Helper()
	source := NewMaterializedSource([]DerivedColumn{{Name: "id"}, {Name: "body"}})
	for index := 0; index < 256; index++ {
		if _, err := source.Insert(Row{
			"id":   int64(index),
			"body": "alpha beta document " + strconv.Itoa(index%32),
		}); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := source.BuildTextIndex("body"); err != nil {
		b.Fatal(err)
	}
	return source
}

func BenchmarkTT024TextIndexCatalogPersist(b *testing.B) {
	source := benchmarkTT024TextIndexCatalogSource(b)
	catalog := &tt024TextIndexCatalogBenchmarkMemory{}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := source.PersistTextIndex(ctx, catalog, "docs", "body"); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(catalog.frame)), "frame-bytes")
}

func BenchmarkTT024TextIndexMarshalDirect(b *testing.B) {
	source := benchmarkTT024TextIndexCatalogSource(b)
	var frame []byte
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var err error
		frame, err = source.MarshalTextIndex("body")
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(frame)), "frame-bytes")
}

func BenchmarkTT024TextIndexCatalogRestore(b *testing.B) {
	source := benchmarkTT024TextIndexCatalogSource(b)
	catalog := &tt024TextIndexCatalogBenchmarkMemory{}
	if err := source.PersistTextIndex(context.Background(), catalog, "docs", "body"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := source.RestoreTextIndexFromCatalog(context.Background(), catalog, "docs", "body"); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(catalog.frame)), "frame-bytes")
}

func BenchmarkTT024TextIndexRestoreDirect(b *testing.B) {
	source := benchmarkTT024TextIndexCatalogSource(b)
	frame, err := source.MarshalTextIndex("body")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := source.RestoreTextIndex("body", frame); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(frame)), "frame-bytes")
}
