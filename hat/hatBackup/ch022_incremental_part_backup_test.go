package hatBackup

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type ch022ObjectStore struct {
	objects      map[string][]byte
	putKeys      []string
	putBytes     int64
	payloadBytes int64
	forceMissing bool
}

func newCH022ObjectStore() *ch022ObjectStore {
	return &ch022ObjectStore{objects: make(map[string][]byte)}
}

func (store *ch022ObjectStore) Put(_ context.Context, key string, body io.Reader, size int64) error {
	payload, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	if int64(len(payload)) != size {
		return fmt.Errorf("put %q has %d bytes, want %d", key, len(payload), size)
	}
	store.objects[key] = payload
	store.putKeys = append(store.putKeys, key)
	store.putBytes += int64(len(payload))
	if !strings.HasSuffix(key, "/manifest.json") && key != "manifest.json" {
		store.payloadBytes += int64(len(payload))
	}
	return nil
}

func (store *ch022ObjectStore) Get(_ context.Context, key string) (io.ReadCloser, error) {
	payload, ok := store.objects[key]
	if !ok {
		return nil, fmt.Errorf("object %q is missing", key)
	}
	return io.NopCloser(bytes.NewReader(payload)), nil
}

func (store *ch022ObjectStore) Exists(_ context.Context, key string) (bool, error) {
	if store.forceMissing {
		return false, nil
	}
	_, ok := store.objects[key]
	return ok, nil
}

func (store *ch022ObjectStore) payloadPutCount() int {
	count := 0
	for _, key := range store.putKeys {
		if !strings.HasSuffix(key, "/manifest.json") && key != "manifest.json" {
			count++
		}
	}
	return count
}

func TestCH022ContentAddressedIncrementalBackupDeduplicatesAndRestores(t *testing.T) {
	root := t.TempDir()
	writeCH022File(t, filepath.Join(root, "part-a", "rows.bin"), []byte("same-part"))
	writeCH022File(t, filepath.Join(root, "part-b", "rows.bin"), []byte("same-part"))
	store := newCH022ObjectStore()
	target, err := NewObjectStoreTargetWithOptions(store, "backup", ObjectStoreTargetOptions{
		Layout: ObjectStoreLayoutContentAddressed,
	})
	if err != nil {
		t.Fatal(err)
	}

	base, err := target.Backup(context.Background(), root, BundleManifest{
		Mode:     ModePebbleIncremental,
		BackupID: "base",
	})
	if err != nil {
		t.Fatalf("base Backup() error = %v", err)
	}
	if base.ObjectLayout != string(ObjectStoreLayoutContentAddressed) || base.NewObjects != 1 || len(base.NewObjectHashes) != 1 {
		t.Fatalf("base accounting = layout:%q new:%d hashes:%v", base.ObjectLayout, base.NewObjects, base.NewObjectHashes)
	}
	if store.payloadPutCount() != 1 {
		t.Fatalf("base payload puts = %d, want one deduplicated object", store.payloadPutCount())
	}

	writeCH022File(t, filepath.Join(root, "part-b", "rows.bin"), []byte("changed-part"))
	next, err := target.Backup(context.Background(), root, BundleManifest{
		Mode:           ModePebbleIncremental,
		BackupID:       "next",
		ParentBackupID: "base",
		Incremental:    true,
	})
	if err != nil {
		t.Fatalf("incremental Backup() error = %v", err)
	}
	if next.NewObjects != 1 || next.ReusedObjects != 1 || len(next.NewObjectHashes) != 1 || len(next.ReusedObjectHashes) != 1 {
		t.Fatalf("incremental accounting = new:%d reused:%d newHashes:%v reusedHashes:%v", next.NewObjects, next.ReusedObjects, next.NewObjectHashes, next.ReusedObjectHashes)
	}
	if store.payloadPutCount() != 2 {
		t.Fatalf("incremental payload puts = %d, want only changed object uploaded", store.payloadPutCount())
	}

	destination := filepath.Join(t.TempDir(), "restore")
	restored, err := target.Restore(context.Background(), destination, false)
	if err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	if restored.BackupID != "next" || restored.ObjectLayout != string(ObjectStoreLayoutContentAddressed) {
		t.Fatalf("restored manifest = %+v", restored)
	}
	for relative, want := range map[string]string{
		"part-a/rows.bin": "same-part",
		"part-b/rows.bin": "changed-part",
	} {
		got, readErr := os.ReadFile(filepath.Join(destination, filepath.FromSlash(relative)))
		if readErr != nil || string(got) != want {
			t.Fatalf("restored %s = %q/%v, want %q", relative, got, readErr, want)
		}
	}
	if _, err := target.Verify(context.Background()); err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
}

func TestCH022AutoIncrementalLayoutAndPathFallback(t *testing.T) {
	root := t.TempDir()
	writeCH022File(t, filepath.Join(root, "part", "rows.bin"), []byte("payload"))

	contentStore := newCH022ObjectStore()
	contentTarget, err := NewObjectStoreTarget(contentStore, "content")
	if err != nil {
		t.Fatal(err)
	}
	contentManifest, err := contentTarget.Backup(context.Background(), root, BundleManifest{Mode: ModePebbleIncremental, BackupID: "auto"})
	if err != nil {
		t.Fatal(err)
	}
	if contentManifest.ObjectLayout != string(ObjectStoreLayoutContentAddressed) {
		t.Fatalf("automatic incremental layout = %q, want content-addressed", contentManifest.ObjectLayout)
	}

	pathStore := newCH022ObjectStore()
	pathTarget, err := NewObjectStoreTargetWithOptions(pathStore, "path", ObjectStoreTargetOptions{Layout: ObjectStoreLayoutPath})
	if err != nil {
		t.Fatal(err)
	}
	pathManifest, err := pathTarget.Backup(context.Background(), root, BundleManifest{Mode: ModePebbleIncremental, BackupID: "path"})
	if err != nil {
		t.Fatal(err)
	}
	if pathManifest.ObjectLayout != string(ObjectStoreLayoutPath) {
		t.Fatalf("path fallback layout = %q, want path", pathManifest.ObjectLayout)
	}
	if _, ok := pathStore.objects["path/part/rows.bin"]; !ok {
		t.Fatal("path fallback did not retain the legacy object key")
	}
}

func TestCH022EncryptedContentAddressedDuplicateRestores(t *testing.T) {
	root := t.TempDir()
	writeCH022File(t, filepath.Join(root, "part-a", "rows.bin"), []byte("encrypted-part"))
	writeCH022File(t, filepath.Join(root, "part-b", "rows.bin"), []byte("encrypted-part"))
	store := newCH022ObjectStore()
	target, err := NewObjectStoreTargetWithOptions(store, "encrypted", ObjectStoreTargetOptions{
		Layout: ObjectStoreLayoutContentAddressed,
		EncryptionKeys: []ObjectStoreEncryptionKey{{
			ID:  "key-1",
			Key: bytes.Repeat([]byte{0x42}, 32),
		}},
		ActiveEncryptionKeyID: "key-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := target.Backup(context.Background(), root, BundleManifest{Mode: ModePebbleIncremental, BackupID: "encrypted"})
	if err != nil {
		t.Fatalf("Backup() error = %v", err)
	}
	if manifest.NewObjects != 1 || store.payloadPutCount() != 1 {
		t.Fatalf("encrypted object accounting = new:%d puts:%d", manifest.NewObjects, store.payloadPutCount())
	}
	destination := filepath.Join(t.TempDir(), "restore")
	if _, err := target.Restore(context.Background(), destination, false); err != nil {
		t.Fatalf("encrypted Restore() error = %v", err)
	}
	for _, relative := range []string{"part-a/rows.bin", "part-b/rows.bin"} {
		got, err := os.ReadFile(filepath.Join(destination, filepath.FromSlash(relative)))
		if err != nil || string(got) != "encrypted-part" {
			t.Fatalf("encrypted restored %s = %q/%v", relative, got, err)
		}
	}
}

func TestCH022EncryptedContentAddressedKeyRotationDoesNotReuseCiphertext(t *testing.T) {
	root := t.TempDir()
	writeCH022File(t, filepath.Join(root, "part", "rows.bin"), []byte("rotated-part"))
	store := newCH022ObjectStore()
	oldTarget, err := NewObjectStoreTargetWithOptions(store, "rotated", ObjectStoreTargetOptions{
		Layout: ObjectStoreLayoutContentAddressed,
		EncryptionKeys: []ObjectStoreEncryptionKey{{
			ID:  "key-old",
			Key: bytes.Repeat([]byte{0x11}, 32),
		}},
		ActiveEncryptionKeyID: "key-old",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := oldTarget.Backup(context.Background(), root, BundleManifest{Mode: ModePebbleIncremental, BackupID: "old"}); err != nil {
		t.Fatalf("old Backup() error = %v", err)
	}
	newTarget, err := NewObjectStoreTargetWithOptions(store, "rotated", ObjectStoreTargetOptions{
		Layout: ObjectStoreLayoutContentAddressed,
		EncryptionKeys: []ObjectStoreEncryptionKey{{
			ID:  "key-new",
			Key: bytes.Repeat([]byte{0x22}, 32),
		}},
		ActiveEncryptionKeyID: "key-new",
	})
	if err != nil {
		t.Fatal(err)
	}
	next, err := newTarget.Backup(context.Background(), root, BundleManifest{
		Mode:           ModePebbleIncremental,
		BackupID:       "new",
		ParentBackupID: "old",
		Incremental:    true,
	})
	if err != nil {
		t.Fatalf("new Backup() error = %v", err)
	}
	if next.NewObjects != 1 || next.ReusedObjects != 0 {
		t.Fatalf("rotated object accounting = new:%d reused:%d", next.NewObjects, next.ReusedObjects)
	}
	destination := filepath.Join(t.TempDir(), "restore")
	if _, err := newTarget.Restore(context.Background(), destination, false); err != nil {
		t.Fatalf("rotated Restore() error = %v", err)
	}
	got, err := os.ReadFile(filepath.Join(destination, "part", "rows.bin"))
	if err != nil || string(got) != "rotated-part" {
		t.Fatalf("rotated restored payload = %q/%v", got, err)
	}
}

func TestCH022EncryptedContentAddressedRetentionUsesPhysicalObjectKeys(t *testing.T) {
	hash := strings.Repeat("a", 64)
	file := BundleFile{Path: "part-000", Size: 1, SHA256: hash}
	base := BundleManifest{
		Version:           BundleVersion,
		Mode:              ModePebbleIncremental,
		BackupID:          "base",
		Store:             "store",
		StorageBackend:    "pebble",
		StorageFormat:     "sst",
		StorageIdentity:   "db",
		StorageGeneration: 1,
		ObjectLayout:      string(ObjectStoreLayoutContentAddressed),
		Encryption:        encryptionMetadataForKey("key-old"),
		Files:             []BundleFile{file},
	}
	rotated := base
	rotated.BackupID = "rotated"
	rotated.ParentBackupID = base.BackupID
	rotated.Incremental = true
	rotated.Encryption = encryptionMetadataForKey("key-new")

	plan, err := PlanBackupRetention([]BundleManifest{base, rotated}, rotated.BackupID, 1)
	if err != nil {
		t.Fatalf("PlanBackupRetention() error = %v", err)
	}
	wantKeep := objectStoreContentPrefix + "/key-new/" + hash
	wantDelete := objectStoreContentPrefix + "/key-old/" + hash
	if len(plan.KeepObjectKeys) != 1 || plan.KeepObjectKeys[0] != wantKeep {
		t.Fatalf("KeepObjectKeys = %#v, want [%q]", plan.KeepObjectKeys, wantKeep)
	}
	if len(plan.DeleteObjectKeys) != 1 || plan.DeleteObjectKeys[0] != wantDelete {
		t.Fatalf("DeleteObjectKeys = %#v, want [%q]", plan.DeleteObjectKeys, wantDelete)
	}
	if plan.Chain.ObjectCount != 2 {
		t.Fatalf("chain object count = %d, want 2 physical objects", plan.Chain.ObjectCount)
	}
}

func writeCH022File(t testing.TB, path string, payload []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
}
