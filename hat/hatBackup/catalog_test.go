package hatBackup

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestBackupManifestCatalogPersistsAndPlansChain(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.json")
	catalog, err := NewBackupManifestCatalog(path)
	if err != nil {
		t.Fatalf("NewBackupManifestCatalog() error = %v", err)
	}
	base := catalogTestManifest("base", "", false, 10, "base-object")
	child := catalogTestManifest("child", "base", true, 20, "child-object")
	if err := catalog.Append(base); err != nil {
		t.Fatalf("Append(base) error = %v", err)
	}
	if err := catalog.Append(child); err != nil {
		t.Fatalf("Append(child) error = %v", err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read catalog = %v", err)
	}
	if !bytes.HasPrefix(contents, []byte(backupManifestCatalogLogHeader)) {
		t.Fatalf("catalog is not append-log encoded: %q", contents)
	}
	manifests, err := catalog.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got := []string{manifests[0].BackupID, manifests[1].BackupID}; !reflect.DeepEqual(got, []string{"base", "child"}) {
		t.Fatalf("manifest order = %#v", got)
	}
	plan, err := catalog.Plan("child")
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if plan.BaseBackupID != "base" || plan.LatestBackupID != "child" || plan.JournalSequenceStart != 10 || plan.JournalSequenceEnd != 20 {
		t.Fatalf("chain plan = %#v", plan)
	}

	reopened, err := NewBackupManifestCatalog(path)
	if err != nil {
		t.Fatalf("reopen NewBackupManifestCatalog() error = %v", err)
	}
	reopenedManifests, err := reopened.Load()
	if err != nil {
		t.Fatalf("reopened Load() error = %v", err)
	}
	if !reflect.DeepEqual(manifests, reopenedManifests) {
		t.Fatalf("reopened manifests = %#v, want %#v", reopenedManifests, manifests)
	}
	manifests[0].Files[0].Path = "mutated"
	independent, err := reopened.Load()
	if err != nil {
		t.Fatalf("independent Load() error = %v", err)
	}
	if independent[0].Files[0].Path == "mutated" {
		t.Fatal("Load() returned aliased manifest data")
	}
}

func TestBackupManifestCatalogRejectsUnsafeUpdatesWithoutChangingState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.json")
	catalog, err := NewBackupManifestCatalog(path)
	if err != nil {
		t.Fatal(err)
	}
	child := catalogTestManifest("child", "base", true, 20, "child-object")
	if err := catalog.Append(child); err == nil {
		t.Fatal("orphan child was accepted")
	}
	base := catalogTestManifest("base", "", false, 10, "base-object")
	if err := catalog.Append(base); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Append(base); err == nil {
		t.Fatal("duplicate manifest was accepted")
	}
	got, err := catalog.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].BackupID != "base" {
		t.Fatalf("state after rejected append = %#v", got)
	}

	if err := os.WriteFile(path, []byte(`{"version":1,"manifests":[`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Load(); err == nil {
		t.Fatal("malformed catalog was accepted")
	}
}

func TestBackupManifestCatalogReplaceIsAtomicAndValidatesAllManifests(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "catalog.json")
	catalog, err := NewBackupManifestCatalog(path)
	if err != nil {
		t.Fatal(err)
	}
	base := catalogTestManifest("base", "", false, 10, "base-object")
	child := catalogTestManifest("child", "base", true, 20, "child-object")
	if err := catalog.Replace([]BundleManifest{base, child}); err != nil {
		t.Fatalf("Replace() error = %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("catalog file was not published: %v", err)
	}
	if err := catalog.Replace([]BundleManifest{child}); err == nil {
		t.Fatal("Replace() accepted a catalog with a missing parent")
	}
	got, err := catalog.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("state after rejected Replace() = %#v", got)
	}
	cycleCatalog, err := NewBackupManifestCatalog(filepath.Join(t.TempDir(), "cycle.json"))
	if err != nil {
		t.Fatal(err)
	}
	cycleA := catalogTestManifest("cycle-a", "cycle-b", true, 30, "cycle-a-object")
	cycleB := catalogTestManifest("cycle-b", "cycle-a", true, 40, "cycle-b-object")
	if err := cycleCatalog.Replace([]BundleManifest{cycleA, cycleB}); err == nil {
		t.Fatal("Replace() accepted a cyclic parent graph")
	}
}

func TestBackupManifestCatalogRejectsSymlinkPath(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target.json")
	link := filepath.Join(root, "catalog.json")
	if err := os.WriteFile(target, []byte("sentinel"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	catalog, err := NewBackupManifestCatalog(link)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Replace(nil); err == nil {
		t.Fatal("Replace() accepted a symlink catalog path")
	}
	contents, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "sentinel" {
		t.Fatalf("symlink target changed to %q", contents)
	}
}

func TestBackupManifestCatalogMigratesLegacyJSONOnAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.json")
	base := catalogTestManifest("base", "", false, 10, "base-object")
	legacy, err := json.Marshal(backupManifestCatalogEnvelope{Version: backupManifestCatalogVersion, Manifests: []BundleManifest{base}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, legacy, 0o600); err != nil {
		t.Fatal(err)
	}
	catalog, err := NewBackupManifestCatalog(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Append(catalogTestManifest("child", "base", true, 20, "child-object")); err != nil {
		t.Fatalf("Append() legacy error = %v", err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(contents, []byte(backupManifestCatalogLogHeader)) {
		t.Fatalf("legacy catalog was not migrated: %q", contents)
	}
}

func TestBackupManifestCatalogRejectsTornLogRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.json")
	catalog, err := NewBackupManifestCatalog(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Append(catalogTestManifest("base", "", false, 10, "base-object")); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, contents[:len(contents)-1], 0o600); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewBackupManifestCatalog(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Load(); err == nil {
		t.Fatal("torn catalog record was accepted")
	}
}

func catalogTestManifest(id, parent string, incremental bool, sequence uint64, object string) BundleManifest {
	digest := sha256.Sum256([]byte(object))
	return BundleManifest{
		Version:           BundleVersion,
		CreatedAt:         time.Unix(1700000000+int64(sequence), 0).UTC(),
		Mode:              ModePebbleIncremental,
		Store:             "pebble",
		BackupID:          id,
		ParentBackupID:    parent,
		Incremental:       incremental,
		StorageBackend:    "pebble",
		StorageFormat:     "checkpoint",
		StorageIdentity:   "catalog-test",
		StorageGeneration: 1,
		ObjectLayout:      "content-addressed",
		JournalSequence:   sequence,
		Files: []BundleFile{{
			Path:   "objects/" + object,
			Size:   int64(len(object)),
			SHA256: hexDigest(digest[:]),
		}},
	}
}

func hexDigest(value []byte) string {
	const hex = "0123456789abcdef"
	encoded := make([]byte, len(value)*2)
	for index, item := range value {
		encoded[index*2] = hex[item>>4]
		encoded[index*2+1] = hex[item&15]
	}
	return string(encoded)
}
