package hatSql

import (
	"context"
	"path/filepath"
	"testing"
)

func BenchmarkCH004InMemoryFinalSchemaRegistrySnapshotBaseline(b *testing.B) {
	registry, err := NewSQLFinalSchemaRegistry(SQLFinalSchemaRegistryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	for _, registration := range ch004FinalSchemaStoreBenchmarkRegistrations() {
		if err := registry.Upsert(registration.Kind, registration.Key, registration.Definition); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if snapshot := registry.Snapshot(); len(snapshot) != 2 {
			b.Fatal("unexpected snapshot length")
		}
	}
}

func ch004FinalSchemaStoreBenchmarkRegistrations() []SQLFinalSchemaRegistration {
	return []SQLFinalSchemaRegistration{
		{
			Kind: "table",
			Key:  "orders",
			Definition: SQLFinalSchemaDefinition{
				Mode:         SQLFinalReplacing,
				KeyFields:    []string{"tenant", "id"},
				VersionField: "version",
			},
		},
		{
			Kind: "stream",
			Key:  "events",
			Definition: SQLFinalSchemaDefinition{
				Mode:      SQLFinalCollapsing,
				KeyFields: []string{"id"},
				SignField: "sign",
			},
		},
	}
}

func BenchmarkCH004FileFinalSchemaStoreSave(b *testing.B) {
	store, err := NewFileSQLFinalSchemaStore(filepath.Join(b.TempDir(), "final-schema.hfs"))
	if err != nil {
		b.Fatal(err)
	}
	registrations := ch004FinalSchemaStoreBenchmarkRegistrations()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := store.SaveSQLFinalSchemaRegistrations(context.Background(), registrations); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCH004FileFinalSchemaStoreLoad(b *testing.B) {
	store, err := NewFileSQLFinalSchemaStore(filepath.Join(b.TempDir(), "final-schema.hfs"))
	if err != nil {
		b.Fatal(err)
	}
	if err := store.SaveSQLFinalSchemaRegistrations(context.Background(), ch004FinalSchemaStoreBenchmarkRegistrations()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		registrations, err := store.LoadSQLFinalSchemaRegistrations(context.Background())
		if err != nil || len(registrations) != 2 {
			b.Fatalf("LoadSQLFinalSchemaRegistrations() = %d registrations, error %v", len(registrations), err)
		}
	}
}
