package hatSchema

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestMZ048SpaceCatalogManifestStorePublishesAndLoads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "catalog.json")
	store, err := NewSpaceCatalogManifestStore(path, SpaceCatalogManifestStoreOptions{})
	if err != nil {
		t.Fatalf("NewSpaceCatalogManifestStore() error = %v", err)
	}
	manifest := SpaceCatalogManifest{
		Version:    SpaceCatalogManifestVersion,
		Generation: 7,
		Spaces: []SpaceDefinition{
			{Name: "zeta", Version: 2},
			{Name: "alpha", Version: 1},
		},
	}
	if err := store.Publish(manifest); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	got, err := store.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got.Version != SpaceCatalogManifestVersion || got.Generation != 7 {
		t.Fatalf("manifest header = %#v", got)
	}
	if len(got.Spaces) != 2 || got.Spaces[0].Name != "alpha" || got.Spaces[1].Name != "zeta" {
		t.Fatalf("spaces = %#v, want deterministic order", got.Spaces)
	}
	if got.Spaces[0].Source.Name != "alpha" || got.Spaces[1].Source.Name != "zeta" {
		t.Fatalf("source names = %#v, want normalized names", got.Spaces)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if !bytes.Contains(data, []byte(`"format":1`)) {
		t.Fatalf("stored manifest is missing format envelope: %s", data)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if permission := info.Mode().Perm(); permission != 0o600 {
		t.Fatalf("manifest permissions = %o, want 600", permission)
	}
}

func TestMZ048SpaceCatalogManifestMigratesLegacyAndPublishesAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.json")
	legacy := SpaceCatalogManifest{
		Generation: 3,
		Spaces:     []SpaceDefinition{{Name: "orders", Version: 4}},
	}
	legacyData, err := json.Marshal(legacy)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if err := os.WriteFile(path, legacyData, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	store, err := NewSpaceCatalogManifestStore(path, SpaceCatalogManifestStoreOptions{})
	if err != nil {
		t.Fatalf("NewSpaceCatalogManifestStore() error = %v", err)
	}
	got, err := store.LoadAndMigrate()
	if err != nil {
		t.Fatalf("LoadAndMigrate() error = %v", err)
	}
	if got.Version != SpaceCatalogManifestVersion || got.Generation != 3 || len(got.Spaces) != 1 {
		t.Fatalf("migrated manifest = %#v", got)
	}
	if got.Spaces[0].Source.Name != "orders" {
		t.Fatalf("migrated source = %#v, want normalized source name", got.Spaces[0].Source)
	}
	upgraded, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(upgraded) error = %v", err)
	}
	if bytes.Equal(upgraded, legacyData) || !bytes.Contains(upgraded, []byte(`"format":1`)) {
		t.Fatalf("legacy manifest was not atomically upgraded: %s", upgraded)
	}
}

func TestMZ048SpaceCatalogManifestCanonicalizesLegacyCurrentVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.json")
	legacyCurrent := SpaceCatalogManifest{
		Version: SpaceCatalogManifestVersion,
		Spaces:  []SpaceDefinition{{Name: "events"}},
	}
	data, err := json.Marshal(legacyCurrent)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	store, err := NewSpaceCatalogManifestStore(path, SpaceCatalogManifestStoreOptions{})
	if err != nil {
		t.Fatalf("NewSpaceCatalogManifestStore() error = %v", err)
	}
	if _, err := store.LoadAndMigrate(); err != nil {
		t.Fatalf("LoadAndMigrate() error = %v", err)
	}
	canonical, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(canonical) error = %v", err)
	}
	if !bytes.Contains(canonical, []byte(`"format":1`)) {
		t.Fatalf("legacy current-version manifest was not canonicalized: %s", canonical)
	}
}

func TestMZ048SpaceCatalogManifestSupportsRegisteredMigration(t *testing.T) {
	migrator := NewSpaceCatalogManifestMigrator()
	if err := migrator.Register(SpaceCatalogManifestVersion, func(manifest SpaceCatalogManifest) (SpaceCatalogManifest, error) {
		manifest.Version++
		manifest.Generation++
		return manifest, nil
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	path := filepath.Join(t.TempDir(), "catalog.json")
	legacyCurrent := SpaceCatalogManifest{
		Version:    SpaceCatalogManifestVersion,
		Generation: 11,
		Spaces:     []SpaceDefinition{{Name: "events"}},
	}
	data, err := json.Marshal(legacyCurrent)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	store, err := NewSpaceCatalogManifestStore(path, SpaceCatalogManifestStoreOptions{
		Migrator:      migrator,
		TargetVersion: SpaceCatalogManifestVersion + 1,
	})
	if err != nil {
		t.Fatalf("NewSpaceCatalogManifestStore() error = %v", err)
	}
	got, err := store.LoadAndMigrate()
	if err != nil {
		t.Fatalf("LoadAndMigrate() error = %v", err)
	}
	if got.Version != SpaceCatalogManifestVersion+1 || got.Generation != 12 {
		t.Fatalf("custom migration result = %#v", got)
	}
}

func TestMZ048SpaceCatalogManifestRejectsTamperingAndPreservesState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.json")
	store, err := NewSpaceCatalogManifestStore(path, SpaceCatalogManifestStoreOptions{})
	if err != nil {
		t.Fatalf("NewSpaceCatalogManifestStore() error = %v", err)
	}
	valid := SpaceCatalogManifest{Version: SpaceCatalogManifestVersion, Spaces: []SpaceDefinition{{Name: "orders"}}}
	if err := store.Publish(valid); err != nil {
		t.Fatalf("Publish(valid) error = %v", err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(before) error = %v", err)
	}
	tampered := bytes.Replace(before, []byte("orders"), []byte("xrders"), 1)
	if bytes.Equal(tampered, before) {
		t.Fatal("test fixture did not contain the expected catalog name")
	}
	if err := os.WriteFile(path, tampered, 0o600); err != nil {
		t.Fatalf("WriteFile(tampered) error = %v", err)
	}
	if _, err := store.Load(); !errors.Is(err, ErrSpaceCatalogManifestChecksum) {
		t.Fatalf("Load(tampered) error = %v, want checksum error", err)
	}

	if err := store.Publish(SpaceCatalogManifest{Version: SpaceCatalogManifestVersion, Spaces: []SpaceDefinition{{Name: ""}}}); !errors.Is(err, ErrSpaceCatalogNameRequired) {
		t.Fatalf("Publish(invalid) error = %v, want name error", err)
	}
}

func TestMZ048SpaceCatalogManifestMigrationValidation(t *testing.T) {
	migrator := NewSpaceCatalogManifestMigrator()
	if err := migrator.Register(SpaceCatalogManifestVersion, nil); !errors.Is(err, ErrSpaceCatalogManifestMigrationInvalid) {
		t.Fatalf("Register(nil) error = %v", err)
	}
	if err := migrator.Register(SpaceCatalogManifestVersion, func(manifest SpaceCatalogManifest) (SpaceCatalogManifest, error) {
		return manifest, nil
	}); err != nil {
		t.Fatalf("Register(first) error = %v", err)
	}
	if err := migrator.Register(SpaceCatalogManifestVersion, func(manifest SpaceCatalogManifest) (SpaceCatalogManifest, error) {
		return manifest, nil
	}); !errors.Is(err, ErrSpaceCatalogManifestMigrationExists) {
		t.Fatalf("Register(duplicate) error = %v", err)
	}
	missing := NewSpaceCatalogManifestMigrator()
	if _, err := missing.Migrate(SpaceCatalogManifest{Version: SpaceCatalogManifestVersion}, SpaceCatalogManifestVersion+2); !errors.Is(err, ErrSpaceCatalogManifestMigrationMissing) {
		t.Fatalf("Migrate(missing) error = %v, want missing migration", err)
	}
	if _, err := migrator.Migrate(SpaceCatalogManifest{Version: SpaceCatalogManifestVersion + 1}, SpaceCatalogManifestVersion); !errors.Is(err, ErrSpaceCatalogManifestVersionInvalid) {
		t.Fatalf("Migrate(regression) error = %v, want version error", err)
	}
}

func TestMZ048SpaceCatalogManifestFailedMigrationPreservesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.json")
	baseStore, err := NewSpaceCatalogManifestStore(path, SpaceCatalogManifestStoreOptions{})
	if err != nil {
		t.Fatalf("NewSpaceCatalogManifestStore(base) error = %v", err)
	}
	if err := baseStore.Publish(SpaceCatalogManifest{Version: SpaceCatalogManifestVersion, Spaces: []SpaceDefinition{{Name: "orders"}}}); err != nil {
		t.Fatalf("Publish(base) error = %v", err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(before) error = %v", err)
	}
	migrator := NewSpaceCatalogManifestMigrator()
	wantFailure := errors.New("migration rejected")
	if err := migrator.Register(SpaceCatalogManifestVersion, func(SpaceCatalogManifest) (SpaceCatalogManifest, error) {
		return SpaceCatalogManifest{}, wantFailure
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	store, err := NewSpaceCatalogManifestStore(path, SpaceCatalogManifestStoreOptions{
		Migrator:      migrator,
		TargetVersion: SpaceCatalogManifestVersion + 1,
	})
	if err != nil {
		t.Fatalf("NewSpaceCatalogManifestStore(migration) error = %v", err)
	}
	if _, err := store.LoadAndMigrate(); !errors.Is(err, wantFailure) {
		t.Fatalf("LoadAndMigrate() error = %v, want migration error", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(after) error = %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("failed migration changed the persisted manifest")
	}
}

func TestMZ048SpaceCatalogManifestRejectsSymlinkPath(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "target.json")
	link := filepath.Join(directory, "catalog.json")
	if err := os.WriteFile(target, []byte("{}"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("Symlink() error = %v", err)
	}
	if _, err := NewSpaceCatalogManifestStore(link, SpaceCatalogManifestStoreOptions{}); !errors.Is(err, ErrSpaceCatalogManifestPath) {
		t.Fatalf("NewSpaceCatalogManifestStore(symlink) error = %v, want path error", err)
	}
}
