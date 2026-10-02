package hatSchema

import "testing"

// BenchmarkTU21BaselineSpaceCatalogLookup records the existing metadata lookup
// cost before the opt-in migration manager is introduced.
func BenchmarkTU21BaselineSpaceCatalogLookup(b *testing.B) {
	catalog, err := NewSpaceCatalog([]SpaceDefinition{{Name: "orders"}})
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := catalog.Lookup("orders"); !ok {
			b.Fatal("orders was not found")
		}
	}
}
