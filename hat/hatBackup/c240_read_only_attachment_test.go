package hatBackup

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

type c240ObjectStore struct {
	objects map[string][]byte
}

func newC240ObjectStore() *c240ObjectStore {
	return &c240ObjectStore{objects: make(map[string][]byte)}
}

func (store *c240ObjectStore) Put(_ context.Context, key string, body io.Reader, size int64) error {
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	if int64(len(data)) != size {
		return errors.New("object size mismatch")
	}
	store.objects[key] = append([]byte(nil), data...)
	return nil
}

func (store *c240ObjectStore) Get(_ context.Context, key string) (io.ReadCloser, error) {
	data, ok := store.objects[key]
	if !ok {
		return nil, os.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func TestReadOnlyBackupAttachmentStreamsWithoutRestore(t *testing.T) {
	store := newC240ObjectStore()
	target, err := NewObjectStoreTarget(store, "backups/c240")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	payloadPath := filepath.Join(root, "part", "rows.bin")
	if err := os.MkdirAll(filepath.Dir(payloadPath), 0o755); err != nil {
		t.Fatal(err)
	}
	want := []byte("immutable rows\n")
	if err := os.WriteFile(payloadPath, want, 0o600); err != nil {
		t.Fatal(err)
	}
	manifest, err := target.Backup(context.Background(), root, BundleManifest{
		Mode:     ModePebbleIncremental,
		BackupID: "c240",
	})
	if err != nil {
		t.Fatal(err)
	}
	objectCount := len(store.objects)

	attachment, err := target.AttachReadOnly(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := attachment.Manifest().BackupID; got != manifest.BackupID {
		t.Fatalf("Manifest().BackupID = %q, want %q", got, manifest.BackupID)
	}
	reader, err := attachment.Open(context.Background(), "part/rows.bin")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if closeErr := reader.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("attached payload = %q, want %q", got, want)
	}
	if err := attachment.Verify(context.Background()); err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if len(store.objects) != objectCount {
		t.Fatalf("read-only attachment changed object count from %d to %d", objectCount, len(store.objects))
	}
	if _, err := os.Stat(filepath.Join(root, "restored")); !os.IsNotExist(err) {
		t.Fatalf("attachment unexpectedly created restore path, stat error = %v", err)
	}
}

func TestReadOnlyBackupAttachmentRejectsUnsafeAndUnknownPaths(t *testing.T) {
	store := newC240ObjectStore()
	target, err := NewObjectStoreTarget(store, "backups/c240")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "rows.bin"), []byte("rows"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := target.Backup(context.Background(), root, BundleManifest{BackupID: "c240"}); err != nil {
		t.Fatal(err)
	}
	attachment, err := target.AttachReadOnly(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"missing.bin", "../rows.bin", "/rows.bin", "./rows.bin", "part/../rows.bin", "rows\\bin"} {
		if _, err := attachment.Open(context.Background(), name); err == nil {
			t.Fatalf("Open(%q) unexpectedly succeeded", name)
		}
	}
}

func TestReadOnlyBackupAttachmentDetectsCorruption(t *testing.T) {
	store := newC240ObjectStore()
	target, err := NewObjectStoreTarget(store, "backups/c240")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "rows.bin"), []byte("rows"), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest, err := target.Backup(context.Background(), root, BundleManifest{BackupID: "c240"})
	if err != nil {
		t.Fatal(err)
	}
	attachment, err := target.AttachReadOnly(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	file := manifest.Files[0]
	layout, err := restoreObjectStoreLayout(manifest)
	if err != nil {
		t.Fatal(err)
	}
	objectKey, _, err := target.fileObjectKey(layout, file, manifest)
	if err != nil {
		t.Fatal(err)
	}
	store.objects[objectKey] = []byte("roxs")
	if _, err := attachment.ReadFile(context.Background(), file.Path); err == nil {
		t.Fatal("ReadFile() unexpectedly accepted a corrupted object")
	}
}
