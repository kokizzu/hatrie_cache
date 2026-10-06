package hatBackup

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

type c240MemoryObjectStore struct {
	objects  map[string][]byte
	getCount int
}

func newC240MemoryObjectStore() *c240MemoryObjectStore {
	return &c240MemoryObjectStore{objects: make(map[string][]byte)}
}

func (store *c240MemoryObjectStore) Put(_ context.Context, key string, body io.Reader, _ int64) error {
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	store.objects[key] = append([]byte(nil), data...)
	return nil
}

func (store *c240MemoryObjectStore) Get(_ context.Context, key string) (io.ReadCloser, error) {
	store.getCount++
	data, ok := store.objects[key]
	if !ok {
		return nil, fs.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func TestC240ReadOnlyBackupStreamsVerifiedFilesWithoutRestore(t *testing.T) {
	store := newC240MemoryObjectStore()
	target, err := NewObjectStoreTarget(store, "backup")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	writeTestFile(t, root, "data/value", []byte("read-only backup payload"))

	wantManifest, err := target.Backup(context.Background(), root, BundleManifest{BackupID: "c240"})
	if err != nil {
		t.Fatal(err)
	}
	view, err := target.OpenReadOnly(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if store.getCount != 1 {
		t.Fatalf("OpenReadOnly() fetched %d objects, want only the manifest", store.getCount)
	}
	if got := view.Manifest().BackupID; got != wantManifest.BackupID {
		t.Fatalf("Manifest().BackupID = %q, want %q", got, wantManifest.BackupID)
	}
	files := view.Files()
	if len(files) != 1 || files[0].Path != "data/value" {
		t.Fatalf("Files() = %#v, want the single data/value file", files)
	}
	files[0].Path = "mutated"
	if got := view.Files()[0].Path; got != "data/value" {
		t.Fatalf("Files() returned shared metadata, path became %q", got)
	}
	manifestCopy := view.Manifest()
	manifestCopy.Files[0].Path = "mutated"
	if got := view.Manifest().Files[0].Path; got != "data/value" {
		t.Fatalf("Manifest() returned shared metadata, path became %q", got)
	}
	reader, err := view.OpenFile(context.Background(), "data/value")
	if err != nil {
		t.Fatal(err)
	}
	got, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil {
		t.Fatalf("ReadAll() error = %v", readErr)
	}
	if closeErr != nil {
		t.Fatalf("Close() error = %v", closeErr)
	}
	if string(got) != "read-only backup payload" {
		t.Fatalf("ReadAll() = %q, want the backed-up payload", got)
	}
	if store.getCount != 2 {
		t.Fatalf("OpenFile() fetched %d objects, want manifest plus one payload", store.getCount)
	}
}

func TestC240ReadOnlyBackupRejectsCorruptAndUnsafeReads(t *testing.T) {
	store := newC240MemoryObjectStore()
	target, err := NewObjectStoreTarget(store, "backup")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	writeTestFile(t, root, "data/value", []byte("verified payload"))
	manifest, err := target.Backup(context.Background(), root, BundleManifest{BackupID: "c240"})
	if err != nil {
		t.Fatal(err)
	}
	view, err := target.OpenReadOnly(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := view.OpenFile(context.Background(), "../escape"); !errors.Is(err, ErrObjectStoreManifestInvalid) {
		t.Fatalf("OpenFile(traversal) error = %v, want manifest validation error", err)
	}
	if _, err := view.OpenFile(context.Background(), "missing"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("OpenFile(missing) error = %v, want fs.ErrNotExist", err)
	}
	file := manifest.Files[0]
	store.objects["backup/"+file.Path] = []byte("tampered")
	reader, err := view.OpenFile(context.Background(), file.Path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(reader); err == nil {
		t.Fatal("ReadAll(corrupt) error = nil, want checksum error")
	}
	if err := reader.Close(); err == nil {
		t.Fatal("Close(corrupt) error = nil, want checksum error")
	}
}

func TestC240ReadOnlyBackupCloseVerifiesUnreadPayload(t *testing.T) {
	store := newC240MemoryObjectStore()
	target, err := NewObjectStoreTarget(store, "backup")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	writeTestFile(t, root, "data/value", []byte("partial read must still verify"))
	manifest, err := target.Backup(context.Background(), root, BundleManifest{BackupID: "c240"})
	if err != nil {
		t.Fatal(err)
	}
	view, err := target.OpenReadOnly(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	file := manifest.Files[0]
	store.objects["backup/"+file.Path] = []byte("tampered after the first byte")
	reader, err := view.OpenFile(context.Background(), file.Path)
	if err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 1)
	if _, err := reader.Read(buffer); err != nil {
		t.Fatalf("Read(first byte) error = %v", err)
	}
	if err := reader.Close(); err == nil {
		t.Fatal("Close(partial corrupt) error = nil, want checksum error")
	}
}

func writeTestFile(t *testing.T, root, relative string, data []byte) {
	t.Helper()
	path := root + "/" + relative
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}
