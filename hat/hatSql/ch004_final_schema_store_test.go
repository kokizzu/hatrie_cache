package hatSql

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func ch004FinalSchemaStoreRegistrations() []SQLFinalSchemaRegistration {
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

func TestCH004FinalSchemaStoreRoundTripIsDeterministicAndPrivate(t *testing.T) {
	store, err := NewFileSQLFinalSchemaStore(filepath.Join(t.TempDir(), "final-schema.hfs"))
	if err != nil {
		t.Fatalf("NewFileSQLFinalSchemaStore() error = %v", err)
	}
	registrations := ch004FinalSchemaStoreRegistrations()
	if err := store.SaveSQLFinalSchemaRegistrations(context.Background(), registrations); err != nil {
		t.Fatalf("SaveSQLFinalSchemaRegistrations() error = %v", err)
	}
	got, err := store.LoadSQLFinalSchemaRegistrations(context.Background())
	if err != nil {
		t.Fatalf("LoadSQLFinalSchemaRegistrations() error = %v", err)
	}
	want := []SQLFinalSchemaRegistration{registrations[1], registrations[0]}
	want[0].Kind = "STREAM"
	want[1].Kind = "TABLE"
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("loaded registrations = %#v, want %#v", got, want)
	}
	filePath := store.path
	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if permissions := info.Mode().Perm(); permissions != 0o600 {
		t.Fatalf("file permissions = %#o, want 0600", permissions)
	}
	first, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("ReadFile(first) error = %v", err)
	}
	if err := store.SaveSQLFinalSchemaRegistrations(context.Background(), registrations); err != nil {
		t.Fatalf("second SaveSQLFinalSchemaRegistrations() error = %v", err)
	}
	second, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("ReadFile(second) error = %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("snapshot changed across identical saves")
	}
}

func TestCH004FinalSchemaStoreRestoresRegistry(t *testing.T) {
	store, err := NewFileSQLFinalSchemaStore(filepath.Join(t.TempDir(), "final-schema.hfs"))
	if err != nil {
		t.Fatalf("NewFileSQLFinalSchemaStore() error = %v", err)
	}
	original, err := NewSQLFinalSchemaRegistry(SQLFinalSchemaRegistryOptions{})
	if err != nil {
		t.Fatalf("NewSQLFinalSchemaRegistry() error = %v", err)
	}
	for _, registration := range ch004FinalSchemaStoreRegistrations() {
		if err := original.Upsert(registration.Kind, registration.Key, registration.Definition); err != nil {
			t.Fatalf("Upsert() error = %v", err)
		}
	}
	if err := original.SaveToSQLFinalSchemaStore(context.Background(), store); err != nil {
		t.Fatalf("SaveToSQLFinalSchemaStore() error = %v", err)
	}
	restored, err := NewSQLFinalSchemaRegistryFromStore(context.Background(), store, SQLFinalSchemaRegistryOptions{})
	if err != nil {
		t.Fatalf("NewSQLFinalSchemaRegistryFromStore() error = %v", err)
	}
	if got, want := restored.Snapshot(), original.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("restored snapshot = %#v, want %#v", got, want)
	}
	if _, configured, err := restored.Resolve("table", "orders"); err != nil || !configured {
		t.Fatalf("restored Resolve() = configured %v, error %v; want configured", configured, err)
	}
}

func TestCH004FinalSchemaStoreRejectsCorruptionAndPreservesPreviousFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "final-schema.hfs")
	store, err := NewFileSQLFinalSchemaStore(path)
	if err != nil {
		t.Fatalf("NewFileSQLFinalSchemaStore() error = %v", err)
	}
	if err := store.SaveSQLFinalSchemaRegistrations(context.Background(), ch004FinalSchemaStoreRegistrations()); err != nil {
		t.Fatalf("SaveSQLFinalSchemaRegistrations() error = %v", err)
	}
	previous, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(previous) error = %v", err)
	}
	invalid := ch004FinalSchemaStoreRegistrations()
	invalid[0].Definition.Mode = SQLFinalMode(99)
	if err := store.SaveSQLFinalSchemaRegistrations(context.Background(), invalid); !errors.Is(err, ErrSQLFinalSchemaDefinitionInvalid) {
		t.Fatalf("invalid SaveSQLFinalSchemaRegistrations() error = %v, want ErrSQLFinalSchemaDefinitionInvalid", err)
	}
	current, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(current) error = %v", err)
	}
	if !reflect.DeepEqual(current, previous) {
		t.Fatalf("failed save replaced the previous snapshot")
	}
	if err := os.WriteFile(path, []byte("corrupt"), 0o600); err != nil {
		t.Fatalf("WriteFile(corrupt) error = %v", err)
	}
	if _, err := store.LoadSQLFinalSchemaRegistrations(context.Background()); !errors.Is(err, ErrSQLFinalSchemaStoreCorrupt) {
		t.Fatalf("corrupt LoadSQLFinalSchemaRegistrations() error = %v, want ErrSQLFinalSchemaStoreCorrupt", err)
	}
}

func TestCH004FinalSchemaStoreHonorsContextCancellation(t *testing.T) {
	store, err := NewFileSQLFinalSchemaStore(filepath.Join(t.TempDir(), "final-schema.hfs"))
	if err != nil {
		t.Fatalf("NewFileSQLFinalSchemaStore() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.SaveSQLFinalSchemaRegistrations(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled SaveSQLFinalSchemaRegistrations() error = %v, want context.Canceled", err)
	}
}

func TestCH004FinalSchemaStoreEnforcesBoundsAndMissingFilesAreEmpty(t *testing.T) {
	missing, err := NewFileSQLFinalSchemaStore(filepath.Join(t.TempDir(), "missing.hfs"))
	if err != nil {
		t.Fatalf("NewFileSQLFinalSchemaStore() error = %v", err)
	}
	if got, err := missing.LoadSQLFinalSchemaRegistrations(context.Background()); err != nil || got != nil {
		t.Fatalf("missing LoadSQLFinalSchemaRegistrations() = %#v, %v; want nil, nil", got, err)
	}
	if _, err := NewFileSQLFinalSchemaStoreWithOptions("x", SQLFinalSchemaStoreOptions{MaxDefinitions: 0, MaxBytes: 12}); !errors.Is(err, ErrSQLFinalSchemaStoreOptionsInvalid) {
		t.Fatalf("invalid size options error = %v, want ErrSQLFinalSchemaStoreOptionsInvalid", err)
	}
	limited, err := NewFileSQLFinalSchemaStoreWithOptions(filepath.Join(t.TempDir(), "limited.hfs"), SQLFinalSchemaStoreOptions{MaxDefinitions: 1, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("NewFileSQLFinalSchemaStoreWithOptions() error = %v", err)
	}
	if err := limited.SaveSQLFinalSchemaRegistrations(context.Background(), ch004FinalSchemaStoreRegistrations()); err == nil {
		t.Fatal("SaveSQLFinalSchemaRegistrations() error = nil, want definition bound error")
	}
}
