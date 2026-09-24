package hatBackup

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

type c241ObjectStore struct {
	objects  map[string][]byte
	putKeys  []string
	putBytes int64
}

func newC241ObjectStore() *c241ObjectStore {
	return &c241ObjectStore{objects: make(map[string][]byte)}
}

func (store *c241ObjectStore) Put(_ context.Context, key string, body io.Reader, size int64) error {
	if size < 0 {
		return errors.New("negative object size")
	}
	data, err := io.ReadAll(io.LimitReader(body, size+1))
	if err != nil {
		return err
	}
	if int64(len(data)) != size {
		return errors.New("object size mismatch")
	}
	store.objects[key] = append([]byte(nil), data...)
	store.putKeys = append(store.putKeys, key)
	if filepath.Base(key) != "manifest.json" {
		store.putBytes += size
	}
	return nil
}

func (store *c241ObjectStore) Get(_ context.Context, key string) (io.ReadCloser, error) {
	data, ok := store.objects[key]
	if !ok {
		return nil, os.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (store *c241ObjectStore) Exists(_ context.Context, key string) (bool, error) {
	_, ok := store.objects[key]
	return ok, nil
}

func (store *c241ObjectStore) List(_ context.Context, prefix string) ([]ObjectStoreObject, error) {
	objects := make([]ObjectStoreObject, 0, len(store.objects))
	for key, data := range store.objects {
		if len(prefix) == 0 || len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			objects = append(objects, ObjectStoreObject{Key: key, Size: int64(len(data))})
		}
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })
	return objects, nil
}

func (store *c241ObjectStore) Delete(_ context.Context, key string) error {
	delete(store.objects, key)
	return nil
}

func TestC241ChunkedIncrementalBackupDeduplicatesChangedChunks(t *testing.T) {
	ctx := context.Background()
	store := newC241ObjectStore()
	target, err := NewObjectStoreTargetWithOptions(store, "backups/c241", ObjectStoreTargetOptions{
		Layout:    ObjectStoreLayoutContentAddressed,
		ChunkSize: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	path := filepath.Join(root, "part", "rows.bin")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("AAAABBBBCCCC"), 0o600); err != nil {
		t.Fatal(err)
	}
	base, err := target.Backup(ctx, root, c241Manifest("base", "", false))
	if err != nil {
		t.Fatal(err)
	}
	if len(base.Files) != 1 || len(base.Files[0].Chunks) != 3 {
		t.Fatalf("base chunks = %#v, want three chunks", base.Files)
	}
	if base.NewObjects != 3 || base.NewObjectBytes != 12 || base.ReusedObjects != 0 {
		t.Fatalf("base accounting = new:%d/%d reused:%d, want 3/12 and 0", base.NewObjects, base.NewObjectBytes, base.ReusedObjects)
	}
	if store.putBytes != 12 {
		t.Fatalf("base payload bytes = %d, want 12", store.putBytes)
	}

	if err := os.WriteFile(path, []byte("AAAAXBBBCCCC"), 0o600); err != nil {
		t.Fatal(err)
	}
	next, err := target.Backup(ctx, root, c241Manifest("next", "base", true))
	if err != nil {
		t.Fatal(err)
	}
	if next.NewObjects != 1 || next.NewObjectBytes != 4 || next.ReusedObjects != 2 || next.ReusedObjectBytes != 8 {
		t.Fatalf("incremental accounting = new:%d/%d reused:%d/%d, want 1/4 and 2/8", next.NewObjects, next.NewObjectBytes, next.ReusedObjects, next.ReusedObjectBytes)
	}
	if store.putBytes != 16 {
		t.Fatalf("incremental payload bytes = %d, want 16 total", store.putBytes)
	}
	if next.Files[0].Chunks[0].SHA256 != base.Files[0].Chunks[0].SHA256 || next.Files[0].Chunks[2].SHA256 != base.Files[0].Chunks[2].SHA256 {
		t.Fatal("unchanged chunk hashes were not reused")
	}
	if next.Files[0].Chunks[1].SHA256 == base.Files[0].Chunks[1].SHA256 {
		t.Fatal("changed chunk hash was reused")
	}

	destination := filepath.Join(t.TempDir(), "restore")
	if _, err := target.Restore(ctx, destination, false); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(destination, "part", "rows.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "AAAAXBBBCCCC" {
		t.Fatalf("restored payload = %q", got)
	}
	if _, err := target.Verify(ctx); err != nil {
		t.Fatal(err)
	}
	attachment, err := target.AttachReadOnly(ctx)
	if err != nil {
		t.Fatal(err)
	}
	attached, err := attachment.ReadFile(ctx, "part/rows.bin")
	if err != nil {
		t.Fatal(err)
	}
	if string(attached) != string(got) {
		t.Fatalf("attached payload = %q, want %q", attached, got)
	}
	manifestCopy := attachment.Manifest()
	manifestCopy.Files[0].Chunks[0].SHA256 = "corrupted"
	if attachment.Manifest().Files[0].Chunks[0].SHA256 == "corrupted" {
		t.Fatal("attachment manifest exposed mutable chunk metadata")
	}

	chunkRelative, err := contentObjectRelative(next.Files[0].Chunks[1].SHA256, "")
	if err != nil {
		t.Fatal(err)
	}
	chunkKey := target.objectKey(chunkRelative)
	originalChunk := append([]byte(nil), store.objects[chunkKey]...)
	store.objects[chunkKey][0] ^= 1
	if _, err := target.Verify(ctx); err == nil {
		t.Fatal("verify accepted a corrupted chunk")
	}
	corruptRestore := filepath.Join(t.TempDir(), "restore")
	if _, err := target.Restore(ctx, corruptRestore, false); err == nil {
		t.Fatal("restore accepted a corrupted chunk")
	}
	if _, err := attachment.ReadFile(ctx, "part/rows.bin"); err == nil {
		t.Fatal("read-only attachment accepted a corrupted chunk")
	}
	store.objects[chunkKey] = originalChunk
	if _, err := target.Verify(ctx); err != nil {
		t.Fatalf("verify after restoring chunk: %v", err)
	}
	originalSHA := attachment.files["part/rows.bin"].SHA256
	attachment.files["part/rows.bin"] = BundleFile{
		Path:   "part/rows.bin",
		Size:   attachment.files["part/rows.bin"].Size,
		SHA256: "0000000000000000000000000000000000000000000000000000000000000000",
		Chunks: attachment.files["part/rows.bin"].Chunks,
	}
	if _, err := attachment.ReadFile(ctx, "part/rows.bin"); err == nil {
		t.Fatal("read-only attachment accepted an invalid logical file checksum")
	}
	attachment.files["part/rows.bin"] = BundleFile{
		Path:   "part/rows.bin",
		Size:   attachment.files["part/rows.bin"].Size,
		SHA256: originalSHA,
		Chunks: attachment.files["part/rows.bin"].Chunks,
	}

	retention, err := PlanBackupRetention([]BundleManifest{base, next}, "next", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(retention.KeepObjectHashes) != 3 {
		t.Fatalf("retention keep hashes = %d, want three chunk hashes", len(retention.KeepObjectHashes))
	}
	garbage, err := target.PlanGarbageCollection(ctx, retention)
	if err != nil {
		t.Fatal(err)
	}
	if len(garbage.KeepObjectKeys) != 3 || len(garbage.DeleteObjectKeys) != 1 {
		t.Fatalf("garbage plan keep:%d delete:%d, want keep three and delete one", len(garbage.KeepObjectKeys), len(garbage.DeleteObjectKeys))
	}
}

func TestC241EncryptedChunkedBackupRoundTrip(t *testing.T) {
	ctx := context.Background()
	key := bytes.Repeat([]byte{0x41}, 32)
	store := newC241ObjectStore()
	target, err := NewObjectStoreTargetWithOptions(store, "backups/c241-encrypted", ObjectStoreTargetOptions{
		EncryptionKeys:        []ObjectStoreEncryptionKey{{ID: "k1", Key: key}},
		ActiveEncryptionKeyID: "k1",
		Layout:                ObjectStoreLayoutContentAddressed,
		ChunkSize:             4,
	})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	path := filepath.Join(root, "part", "rows.bin")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("AAAABBBBCCCC"), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest, err := target.Backup(ctx, root, c241Manifest("encrypted", "", false))
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Files) != 1 || len(manifest.Files[0].Chunks) != 3 {
		t.Fatalf("encrypted chunks = %#v, want three chunks", manifest.Files)
	}
	if _, err := target.Verify(ctx); err != nil {
		t.Fatal(err)
	}
	attachment, err := target.AttachReadOnly(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data, err := attachment.ReadFile(ctx, "part/rows.bin")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "AAAABBBBCCCC" {
		t.Fatalf("encrypted attached payload = %q", data)
	}
	destination := filepath.Join(t.TempDir(), "restore")
	if _, err := target.Restore(ctx, destination, false); err != nil {
		t.Fatal(err)
	}
	if restored, err := os.ReadFile(filepath.Join(destination, "part", "rows.bin")); err != nil {
		t.Fatal(err)
	} else if string(restored) != "AAAABBBBCCCC" {
		t.Fatalf("encrypted restored payload = %q", restored)
	}
}

func TestC241ChunkSizeValidation(t *testing.T) {
	for _, chunkSize := range []int{-1, MaxObjectStoreChunkSize + 1} {
		_, err := NewObjectStoreTargetWithOptions(newC241ObjectStore(), "backups/c241-invalid", ObjectStoreTargetOptions{
			ChunkSize: chunkSize,
		})
		if !errors.Is(err, ErrObjectStoreChunkSizeInvalid) {
			t.Fatalf("chunk size %d error = %v, want ErrObjectStoreChunkSizeInvalid", chunkSize, err)
		}
	}
}

func c241Manifest(id, parent string, incremental bool) BundleManifest {
	return BundleManifest{
		Mode:              ModePebbleIncremental,
		BackupID:          id,
		ParentBackupID:    parent,
		Incremental:       incremental,
		Store:             "store",
		StorageBackend:    "pebble",
		StorageFormat:     "checkpoint",
		StorageIdentity:   "c241",
		StorageGeneration: 1,
	}
}
