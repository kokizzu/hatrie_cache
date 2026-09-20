package hatSql

import (
	"errors"
	"testing"
)

func TestMU040SchemaRegistryEnforcesFullCompatibility(t *testing.T) {
	registry, err := NewSQLSchemaRegistry(SQLSchemaRegistryOptions{
		Compatibility: SQLSchemaCompatibilityFull,
	})
	if err != nil {
		t.Fatal(err)
	}
	base := SQLSchemaDefinition{
		Source:  "events",
		Version: "v1",
		Columns: []SQLRowBinaryColumn{{Name: "id", Type: SQLRowBinaryInt64}},
	}
	if err := registry.Register(base); err != nil {
		t.Fatal(err)
	}
	compatible := base
	compatible.Version = "v2"
	compatible.Columns = append(append([]SQLRowBinaryColumn(nil), base.Columns...), SQLRowBinaryColumn{Name: "region", Type: SQLRowBinaryString, Nullable: true})
	if err := registry.Register(compatible); err != nil {
		t.Fatalf("register compatible schema: %v", err)
	}
	incompatible := compatible
	incompatible.Version = "v3"
	incompatible.Columns = append([]SQLRowBinaryColumn(nil), compatible.Columns...)
	incompatible.Columns[0].Type = SQLRowBinaryString
	if err := registry.Register(incompatible); !errors.Is(err, ErrSQLSchemaIncompatible) {
		t.Fatalf("register incompatible schema error = %v, want %v", err, ErrSQLSchemaIncompatible)
	}
	got, ok := registry.Lookup("events", "v2")
	if !ok || len(got.Columns) != 2 || !got.Columns[1].Nullable {
		t.Fatalf("lookup schema = %#v, %v", got, ok)
	}
	got.Columns[0].Name = "mutated"
	copyOfBase, ok := registry.Lookup("events", "v2")
	if !ok || copyOfBase.Columns[0].Name != "id" {
		t.Fatalf("registry returned mutable schema state: %#v, %v", copyOfBase, ok)
	}
}

func TestMU040SchemaRegistryGatesIngestionBeforeApply(t *testing.T) {
	registry, err := NewSQLSchemaRegistry(SQLSchemaRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(SQLSchemaDefinition{
		Source:  "events",
		Version: "v1",
		Columns: []SQLRowBinaryColumn{{Name: "id", Type: SQLRowBinaryInt64}},
	}); err != nil {
		t.Fatal(err)
	}
	coordinator := NewSQLSourceIngestionCoordinatorWithSchemaRegistry(registry)
	called := false
	ingestion := SQLSourceIngestion{
		Source:        "events",
		SchemaVersion: "missing",
		Transaction: SQLSourceTransaction{
			ID:      "tx-1",
			Offsets: []SQLSourceOffset{{Source: "events", Partition: "0", Offset: 1}},
		},
	}
	if applied, err := coordinator.Ingest(ingestion, func() error {
		called = true
		return nil
	}); applied || !errors.Is(err, ErrSQLSchemaVersionUnknown) || called {
		t.Fatalf("unknown schema ingest = applied %v, err %v, called %v", applied, err, called)
	}
	ingestion.SchemaVersion = "v1"
	if applied, err := coordinator.Ingest(ingestion, func() error {
		called = true
		return nil
	}); !applied || err != nil || !called {
		t.Fatalf("registered schema ingest = applied %v, err %v, called %v", applied, err, called)
	}
}

func TestMU040LegacyIngestionRemainsUnchangedWithoutRegistry(t *testing.T) {
	coordinator := NewSQLSourceIngestionCoordinator()
	if applied, err := coordinator.Ingest(SQLSourceIngestion{
		Source: "events",
		Transaction: SQLSourceTransaction{
			ID:      "tx-legacy",
			Offsets: []SQLSourceOffset{{Source: "events", Partition: "0", Offset: 1}},
		},
	}, func() error { return nil }); !applied || err != nil {
		t.Fatalf("legacy ingestion = applied %v, err %v", applied, err)
	}
}

func TestMU040SchemaRegistryDefaultPolicyAndSnapshotRestore(t *testing.T) {
	registry, err := NewSQLSchemaRegistry(SQLSchemaRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	base := SQLSchemaDefinition{
		Source:  "events",
		Version: "1",
		Columns: []SQLRowBinaryColumn{{Name: "id", Type: SQLRowBinaryInt64}},
	}
	if err := registry.Register(base); err != nil {
		t.Fatal(err)
	}
	added := base
	added.Version = "2"
	added.Columns = append(append([]SQLRowBinaryColumn(nil), base.Columns...), SQLRowBinaryColumn{Name: "optional", Type: SQLRowBinaryString, Nullable: true})
	if err := registry.Register(added); err != nil {
		t.Fatalf("default backward-compatible registration: %v", err)
	}
	broken := added
	broken.Version = "3"
	broken.Columns = []SQLRowBinaryColumn{{Name: "optional", Type: SQLRowBinaryString, Nullable: true}}
	if err := registry.Register(broken); !errors.Is(err, ErrSQLSchemaIncompatible) {
		t.Fatalf("default required-column removal error = %v, want %v", err, ErrSQLSchemaIncompatible)
	}
	snapshot := registry.Snapshot()
	restored, err := NewSQLSchemaRegistry(SQLSchemaRegistryOptions{Compatibility: SQLSchemaCompatibilityBackward})
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Restore(snapshot); err != nil {
		t.Fatalf("restore schema registry: %v", err)
	}
	if got := restored.Versions("events"); len(got) != 2 || got[0] != "1" || got[1] != "2" {
		t.Fatalf("restored versions = %#v", got)
	}
	invalidSnapshot := append(append([]SQLSchemaDefinition(nil), snapshot...), SQLSchemaDefinition{
		Source:  "events",
		Version: "3",
		Columns: []SQLRowBinaryColumn{{Name: "id", Type: SQLRowBinaryString}},
	})
	if err := restored.Restore(invalidSnapshot); !errors.Is(err, ErrSQLSchemaIncompatible) {
		t.Fatalf("invalid restore error = %v, want %v", err, ErrSQLSchemaIncompatible)
	}
	if got := restored.Versions("events"); len(got) != 2 {
		t.Fatalf("failed restore changed registry = %#v", got)
	}
}

func TestMU040SchemaRegistryPoliciesAndBounds(t *testing.T) {
	base := SQLSchemaDefinition{
		Source:  "events",
		Version: "1",
		Columns: []SQLRowBinaryColumn{
			{Name: "id", Type: SQLRowBinaryInt64, Nullable: true},
			{Name: "tag", Type: SQLRowBinaryString, Nullable: true},
		},
	}
	forward, err := NewSQLSchemaRegistry(SQLSchemaRegistryOptions{Compatibility: SQLSchemaCompatibilityForward})
	if err != nil {
		t.Fatal(err)
	}
	if err := forward.Register(base); err != nil {
		t.Fatal(err)
	}
	removed := base
	removed.Version = "2"
	removed.Columns = removed.Columns[:1]
	if err := forward.Register(removed); err != nil {
		t.Fatalf("nullable-column removal under forward compatibility: %v", err)
	}
	if _, err := NewSQLSchemaRegistry(SQLSchemaRegistryOptions{MaxColumns: -1}); !errors.Is(err, ErrSQLSchemaDefinitionInvalid) {
		t.Fatalf("negative MaxColumns error = %v, want %v", err, ErrSQLSchemaDefinitionInvalid)
	}
	invalid := base
	invalid.Version = "bad\nversion"
	if err := forward.Register(invalid); !errors.Is(err, ErrSQLSchemaDefinitionInvalid) {
		t.Fatalf("invalid version error = %v, want %v", err, ErrSQLSchemaDefinitionInvalid)
	}
}

func TestMU040SchemaVersionParticipatesInIngestionDeduplicationAndRestore(t *testing.T) {
	registry, err := NewSQLSchemaRegistry(SQLSchemaRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, version := range []string{"v1", "v2"} {
		if err := registry.Register(SQLSchemaDefinition{
			Source:  "events",
			Version: version,
			Columns: []SQLRowBinaryColumn{{Name: "id", Type: SQLRowBinaryInt64}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	coordinator := NewSQLSourceIngestionCoordinatorWithSchemaRegistry(registry)
	base := SQLSourceIngestion{
		Source:        "events",
		SchemaVersion: "v1",
		Transaction: SQLSourceTransaction{
			ID:      "tx-1",
			Offsets: []SQLSourceOffset{{Source: "events", Partition: "0", Offset: 1}},
		},
	}
	if applied, err := coordinator.Ingest(base, func() error { return nil }); !applied || err != nil {
		t.Fatalf("initial ingestion = applied %v, err %v", applied, err)
	}
	conflict := base
	conflict.SchemaVersion = "v2"
	if applied, err := coordinator.Ingest(conflict, func() error { return nil }); applied || !errors.Is(err, ErrSQLSourceIngestionConflict) {
		t.Fatalf("schema-version conflict = applied %v, err %v", applied, err)
	}
	snapshot := coordinator.Snapshot()
	restored := NewSQLSourceIngestionCoordinatorWithSchemaRegistry(registry)
	if err := restored.Restore(snapshot); err != nil {
		t.Fatalf("restore versioned ingestion: %v", err)
	}
	if len(restored.Snapshot()) != 1 || restored.Snapshot()[0].SchemaVersion != "v1" {
		t.Fatalf("restored ingestion snapshot = %#v", restored.Snapshot())
	}
}

func BenchmarkMU040LegacyIngestionBaseline(b *testing.B) {
	benchmarkMU040Ingestion(b, nil)
}

func BenchmarkMU040RegistryIngestion(b *testing.B) {
	registry, err := NewSQLSchemaRegistry(SQLSchemaRegistryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if err := registry.Register(SQLSchemaDefinition{
		Source:  "events",
		Version: "v1",
		Columns: []SQLRowBinaryColumn{{Name: "id", Type: SQLRowBinaryInt64}},
	}); err != nil {
		b.Fatal(err)
	}
	benchmarkMU040Ingestion(b, registry)
}

func benchmarkMU040Ingestion(b *testing.B, registry *SQLSchemaRegistry) {
	const window = 256
	ids := make([]string, window)
	for index := range ids {
		ids[index] = string(rune('a'+index%26)) + string(rune('A'+index/26))
	}
	ingestion := SQLSourceIngestion{
		Source: "events",
		Transaction: SQLSourceTransaction{
			Offsets: []SQLSourceOffset{{Source: "events", Partition: "0", Offset: 1}},
		},
	}
	if registry != nil {
		ingestion.SchemaVersion = "v1"
	}
	coordinator := NewSQLSourceIngestionCoordinatorWithSchemaRegistry(registry)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if index%window == 0 {
			coordinator = NewSQLSourceIngestionCoordinatorWithSchemaRegistry(registry)
		}
		ingestion.Transaction.ID = ids[index%window]
		if _, err := coordinator.Ingest(ingestion, func() error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
}
