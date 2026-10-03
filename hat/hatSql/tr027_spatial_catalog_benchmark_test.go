package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSchema"
	"hatrie_cache/hat/hatSql"
)

func tr027CatalogBenchmarkDefinition() hatSchema.SpaceDefinition {
	return hatSchema.SpaceDefinition{
		Name: "points",
		Source: hatSchema.Source{
			Name: "points",
			Columns: []hatSchema.Column{
				{Name: "latitude", Type: hatSchema.TypeNumber},
				{Name: "longitude", Type: hatSchema.TypeNumber},
			},
		},
		Indexes: []hatSchema.IndexDefinition{{
			Name:    "geo",
			Kind:    hatSchema.IndexKindRTree,
			Columns: []string{"latitude", "longitude"},
		}},
	}
}

func BenchmarkTR027SpatialSourceConstruction(b *testing.B) {
	definition := tr027CatalogBenchmarkDefinition()
	catalog, err := hatSchema.NewSpaceCatalog([]hatSchema.SpaceDefinition{definition})
	if err != nil {
		b.Fatal(err)
	}
	b.Run("direct", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := hatSql.NewRTreeSpatialSource(hatSql.RTreeSpatialSourceOptions{
				SourceName:     "CACHE",
				Name:           "points",
				LatitudeField:  "latitude",
				LongitudeField: "longitude",
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("definition", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := hatSchema.NewRTreeSpatialSourceFromSpaceDefinition(definition, hatSql.RTreeSpatialSourceOptions{SourceName: "CACHE"}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("catalog", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := catalog.NewRTreeSpatialSource("points", hatSql.RTreeSpatialSourceOptions{SourceName: "CACHE"}); err != nil {
				b.Fatal(err)
			}
		}
	})
}
