package source_schema_registry_contract_test

import (
	"errors"
	"reflect"
	"sync"
	"testing"

	"hatrie_cache/hat/hatSchema"
)

func TestSourceSchemaRegistry(t *testing.T) {
	registry, err := hatSchema.NewSourceSchemaRegistry(hatSchema.SourceSchemaRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}

	initialSource := sourceSchema("orders", false, false)
	first, err := registry.Register(initialSource, 1)
	if err != nil {
		t.Fatal(err)
	}
	if first.Source != "orders" || first.Version != 1 || first.Fingerprint == "" {
		t.Fatalf("first version = %+v", first)
	}
	if err := registry.Validate("orders", first.Version, first.Fingerprint); err != nil {
		t.Fatalf("validate first version: %v", err)
	}

	duplicate, err := registry.Register(initialSource, 1)
	if err != nil {
		t.Fatalf("idempotent registration: %v", err)
	}
	if !reflect.DeepEqual(first, duplicate) {
		t.Fatalf("duplicate version = %+v, want %+v", duplicate, first)
	}

	second, err := registry.Register(sourceSchema("orders", true, false), 2)
	if err != nil {
		t.Fatalf("compatible registration: %v", err)
	}
	if second.Version != 2 || second.Fingerprint == first.Fingerprint {
		t.Fatalf("second version = %+v", second)
	}
	if err := registry.Validate("orders", second.Version, second.Fingerprint); err != nil {
		t.Fatalf("validate second version: %v", err)
	}

	if _, err := registry.Register(sourceSchema("orders", false, true), 3); !errors.Is(err, hatSchema.ErrSourceSchemaIncompatible) {
		t.Fatalf("incompatible registration error = %v, want ErrSourceSchemaIncompatible", err)
	}
	if err := registry.Validate("orders", second.Version, second.Fingerprint); err != nil {
		t.Fatalf("failed registration changed latest version: %v", err)
	}

	if _, err := registry.Register(sourceSchema("orders", true, false), 1); !errors.Is(err, hatSchema.ErrSourceSchemaVersionConflict) {
		t.Fatalf("conflicting registration error = %v, want ErrSourceSchemaVersionConflict", err)
	}
	if err := registry.Validate("orders", 2, "wrong-fingerprint"); !errors.Is(err, hatSchema.ErrSourceSchemaFingerprintMismatch) {
		t.Fatalf("fingerprint error = %v, want ErrSourceSchemaFingerprintMismatch", err)
	}
	if err := registry.Validate("orders", 99, second.Fingerprint); !errors.Is(err, hatSchema.ErrSourceSchemaUnknownVersion) {
		t.Fatalf("unknown version error = %v, want ErrSourceSchemaUnknownVersion", err)
	}

	resolved, ok := registry.Lookup("orders", 2)
	if !ok || resolved.Fingerprint != second.Fingerprint {
		t.Fatalf("lookup = %+v, %v", resolved, ok)
	}
	resolved.Definition.Columns[0].Name = "mutated"
	resolvedAgain, ok := registry.Lookup("orders", 2)
	if !ok || resolvedAgain.Definition.Columns[0].Name == "mutated" {
		t.Fatalf("lookup returned registry-owned data: %+v, %v", resolvedAgain, ok)
	}

	snapshot := registry.Snapshot()
	if len(snapshot) != 2 || snapshot[0].Version != 1 || snapshot[1].Version != 2 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	snapshot[0].Definition.Columns[0].Name = "mutated"
	resolvedAgain, ok = registry.Lookup("orders", 1)
	if !ok || resolvedAgain.Definition.Columns[0].Name == "mutated" {
		t.Fatalf("snapshot returned registry-owned data: %+v, %v", resolvedAgain, ok)
	}
}

func TestSourceSchemaRegistryBoundsAndPolicy(t *testing.T) {
	if _, err := hatSchema.NewSourceSchemaRegistry(hatSchema.SourceSchemaRegistryOptions{MaxSources: -1}); !errors.Is(err, hatSchema.ErrSourceSchemaRegistryInvalid) {
		t.Fatalf("invalid options error = %v", err)
	}
	registry, err := hatSchema.NewSourceSchemaRegistry(hatSchema.SourceSchemaRegistryOptions{
		MaxSources:           1,
		MaxVersionsPerSource: 2,
		Compatibility:        hatSchema.SourceSchemaCompatibilityAny,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Register(sourceSchema("orders", false, false), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Register(sourceSchema("orders", true, true), 2); err != nil {
		t.Fatalf("any-policy registration: %v", err)
	}
	if _, err := registry.Register(sourceSchema("orders", false, true), 3); !errors.Is(err, hatSchema.ErrSourceSchemaRegistryLimit) {
		t.Fatalf("version limit error = %v", err)
	}
	if _, err := registry.Register(sourceSchema("customers", false, false), 1); !errors.Is(err, hatSchema.ErrSourceSchemaRegistryLimit) {
		t.Fatalf("source limit error = %v", err)
	}
}

func TestSourceSchemaRegistryConcurrentReadersAndRegistration(t *testing.T) {
	registry, err := hatSchema.NewSourceSchemaRegistry(hatSchema.SourceSchemaRegistryOptions{
		MaxVersionsPerSource: 16,
		Compatibility:        hatSchema.SourceSchemaCompatibilityAny,
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := registry.Register(sourceSchema("orders", false, false), 1)
	if err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for iteration := 0; iteration < 1000; iteration++ {
				if err := registry.Validate("orders", first.Version, first.Fingerprint); err != nil {
					t.Errorf("concurrent validation: %v", err)
					return
				}
				if _, ok := registry.Lookup("orders", first.Version); !ok {
					t.Errorf("concurrent lookup failed")
					return
				}
			}
		}()
	}
	for version := uint64(2); version <= 8; version++ {
		if _, err := registry.Register(sourceSchema("orders", false, false), version); err != nil {
			t.Fatalf("concurrent registration version %d: %v", version, err)
		}
	}
	wait.Wait()
}

func sourceSchema(name string, withNullableColumn, withRequiredColumn bool) hatSchema.Source {
	columns := []hatSchema.Column{
		{Name: "id", Type: hatSchema.TypeInteger, NotNull: true},
		{Name: "region", Type: hatSchema.TypeText},
	}
	if withNullableColumn {
		columns = append(columns, hatSchema.Column{Name: "metadata", Type: hatSchema.TypeJSON})
	}
	if withRequiredColumn {
		columns = append(columns, hatSchema.Column{Name: "required", Type: hatSchema.TypeText, NotNull: true})
	}
	return hatSchema.Source{Name: name, Columns: columns}
}
