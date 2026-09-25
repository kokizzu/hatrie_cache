package hatSql

import (
	"context"
	"path/filepath"
	"testing"
)

func ch011ProjectionBenchmarkDefinitions() []MaterializedViewDefinition {
	return []MaterializedViewDefinition{
		{Name: "people_projection", Query: "FROM CACHE('people') SELECT id, name ORDER BY id DESC", Dependencies: []string{"people"}},
		{Name: "people_totals", Query: "FROM CACHE('people') SELECT COUNT(*) AS total", Dependencies: []string{"people"}},
		{Name: "teams_projection", Query: "FROM CACHE('teams') SELECT id, name", Dependencies: []string{"teams"}},
	}
}

func BenchmarkCH011ProjectionDefinitionStoreSave(b *testing.B) {
	store, err := NewFileSQLProjectionDefinitionStore(filepath.Join(b.TempDir(), "projections.spc"))
	if err != nil {
		b.Fatal(err)
	}
	definitions := ch011ProjectionBenchmarkDefinitions()
	if err := store.SaveSQLProjectionDefinitions(context.Background(), definitions); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := store.SaveSQLProjectionDefinitions(context.Background(), definitions); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCH011ProjectionDefinitionStoreLoad(b *testing.B) {
	store, err := NewFileSQLProjectionDefinitionStore(filepath.Join(b.TempDir(), "projections.spc"))
	if err != nil {
		b.Fatal(err)
	}
	if err := store.SaveSQLProjectionDefinitions(context.Background(), ch011ProjectionBenchmarkDefinitions()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := store.LoadSQLProjectionDefinitions(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}
