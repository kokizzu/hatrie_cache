package hatDataStructure

import "testing"

func BenchmarkTU24BeforeConditionalUpsert(b *testing.B) {
	index, err := NewConditionalFunctionalIndex[tu24Record, string](
		func(record tu24Record) string { return record.Tenant },
		func(record tu24Record) bool { return record.Active },
		1,
	)
	if err != nil {
		b.Fatal(err)
	}
	if err := index.Upsert(1, tu24Record{Tenant: "acme", Active: true}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := index.Upsert(1, tu24Record{Tenant: "acme", Active: true, Value: i}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU24BeforeConditionalLookup(b *testing.B) {
	index, err := NewConditionalFunctionalIndex[tu24Record, string](
		func(record tu24Record) string { return record.Tenant },
		func(record tu24Record) bool { return record.Active },
		1,
	)
	if err != nil {
		b.Fatal(err)
	}
	if err := index.Upsert(1, tu24Record{Tenant: "acme", Active: true}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tu24BenchmarkIDs = index.LookupIDs("acme")
	}
}

func BenchmarkTU24AfterCatalogUpsert(b *testing.B) {
	catalog := NewConditionalIndexCatalog[tu24Record, string]()
	if err := catalog.Create(tu24BenchmarkDefinition()); err != nil {
		b.Fatal(err)
	}
	if err := catalog.Upsert("active-by-tenant", 1, tu24Record{Tenant: "acme", Active: true}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := catalog.Upsert("active-by-tenant", 1, tu24Record{Tenant: "acme", Active: true, Value: i}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU24AfterCatalogLookup(b *testing.B) {
	catalog := NewConditionalIndexCatalog[tu24Record, string]()
	if err := catalog.Create(tu24BenchmarkDefinition()); err != nil {
		b.Fatal(err)
	}
	if err := catalog.Upsert("active-by-tenant", 1, tu24Record{Tenant: "acme", Active: true}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tu24BenchmarkIDs, _ = catalog.LookupIDs("active-by-tenant", "acme")
	}
}

var tu24BenchmarkIDs []uint64

func tu24BenchmarkDefinition() ConditionalIndexDefinition[tu24Record, string] {
	return ConditionalIndexDefinition[tu24Record, string]{
		Name:          "active-by-tenant",
		ExtractorName: "tenant",
		PredicateName: "active",
		Extractor:     func(record tu24Record) string { return record.Tenant },
		Predicate:     func(record tu24Record) bool { return record.Active },
	}
}
