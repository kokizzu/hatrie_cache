package hatBackup_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"hatrie_cache/hat/hatBackup"
)

type objectStoreMemory struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newObjectStoreMemory() *objectStoreMemory {
	return &objectStoreMemory{objects: make(map[string][]byte)}
}

func (store *objectStoreMemory) Put(_ context.Context, key string, body io.Reader, size int64) error {
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
	store.mu.Lock()
	store.objects[key] = append([]byte(nil), data...)
	store.mu.Unlock()
	return nil
}

func (store *objectStoreMemory) Get(_ context.Context, key string) (io.ReadCloser, error) {
	store.mu.Lock()
	data, ok := store.objects[key]
	data = append([]byte(nil), data...)
	store.mu.Unlock()
	if !ok {
		return nil, os.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func TestObjectStoreTargetRoundTripsManifestAndNestedFiles(t *testing.T) {
	ctx := context.Background()
	store := newObjectStoreMemory()
	source := t.TempDir()
	writeObjectStoreFile(t, filepath.Join(source, "part-a", "rows.bin"), []byte("row-a\nrow-b\n"))
	writeObjectStoreFile(t, filepath.Join(source, "part-b", "rows.bin"), []byte("row-c\n"))

	target, err := hatBackup.NewObjectStoreTarget(store, "backups/node-a")
	if err != nil {
		t.Fatalf("NewObjectStoreTarget() error = %v", err)
	}
	manifest, err := target.Backup(ctx, source, hatBackup.BundleManifest{Mode: hatBackup.ModeSnapshot, Snapshot: "snapshot-1"})
	if err != nil {
		t.Fatalf("Backup() error = %v", err)
	}
	wantFiles := []hatBackup.BundleFile{
		{Path: "part-a/rows.bin", Size: 12, SHA256: objectStoreSHA256("row-a\nrow-b\n")},
		{Path: "part-b/rows.bin", Size: 6, SHA256: objectStoreSHA256("row-c\n")},
	}
	if manifest.Version != hatBackup.BundleVersion || manifest.Snapshot != "snapshot-1" || !reflect.DeepEqual(manifest.Files, wantFiles) {
		t.Fatalf("Backup() manifest = %+v, want version/snapshot/files %+v", manifest, wantFiles)
	}

	destination := filepath.Join(t.TempDir(), "restored")
	restored, err := target.Restore(ctx, destination, false)
	if err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	if !reflect.DeepEqual(restored.Files, manifest.Files) {
		t.Fatalf("Restore() files = %+v, want %+v", restored.Files, manifest.Files)
	}
	for path, want := range map[string]string{
		"part-a/rows.bin": "row-a\nrow-b\n",
		"part-b/rows.bin": "row-c\n",
	} {
		got, err := os.ReadFile(filepath.Join(destination, filepath.FromSlash(path)))
		if err != nil {
			t.Fatalf("ReadFile(%q) error = %v", path, err)
		}
		if string(got) != want {
			t.Fatalf("restored %q = %q, want %q", path, got, want)
		}
	}
}

func TestObjectStoreTargetRejectsTraversalManifestBeforeWriting(t *testing.T) {
	store := newObjectStoreMemory()
	manifest, err := json.Marshal(hatBackup.BundleManifest{
		Version: hatBackup.BundleVersion,
		Files:   []hatBackup.BundleFile{{Path: "../escape", SHA256: strings.Repeat("0", 64)}},
	})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	store.objects["manifest.json"] = manifest
	target, err := hatBackup.NewObjectStoreTarget(store, "")
	if err != nil {
		t.Fatalf("NewObjectStoreTarget() error = %v", err)
	}
	destination := filepath.Join(t.TempDir(), "restored")
	if _, err := target.Restore(context.Background(), destination, false); err == nil {
		t.Fatal("Restore() accepted a traversal path")
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(destination), "escape")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unexpected escape path state, stat error = %v", err)
	}
}

func TestObjectStoreTargetRejectsChecksumMismatchBeforePublishing(t *testing.T) {
	store := newObjectStoreMemory()
	manifest := hatBackup.BundleManifest{
		Version: hatBackup.BundleVersion,
		Files:   []hatBackup.BundleFile{{Path: "rows.bin", Size: 3, SHA256: objectStoreSHA256("bad")}},
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	store.objects["manifest.json"] = encoded
	store.objects["rows.bin"] = []byte("ok!")
	target, err := hatBackup.NewObjectStoreTarget(store, "")
	if err != nil {
		t.Fatalf("NewObjectStoreTarget() error = %v", err)
	}
	destination := filepath.Join(t.TempDir(), "restored")
	if _, err := target.Restore(context.Background(), destination, false); err == nil {
		t.Fatal("Restore() accepted a checksum mismatch")
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("destination exists after failed restore, stat error = %v", err)
	}
}

func TestObjectStoreTargetValidatesStoreAndPrefix(t *testing.T) {
	if _, err := hatBackup.NewObjectStoreTarget(nil, "backup"); !errors.Is(err, hatBackup.ErrObjectStoreNil) {
		t.Fatalf("nil store error = %v, want ErrObjectStoreNil", err)
	}
	for _, prefix := range []string{"../backup", "/backup", "backup\\node"} {
		if _, err := hatBackup.NewObjectStoreTarget(newObjectStoreMemory(), prefix); !errors.Is(err, hatBackup.ErrObjectStorePrefixInvalid) {
			t.Errorf("prefix %q error = %v, want ErrObjectStorePrefixInvalid", prefix, err)
		}
	}
}

func TestObjectStoreTargetRejectsCancelledContext(t *testing.T) {
	store := newObjectStoreMemory()
	target, err := hatBackup.NewObjectStoreTarget(store, "backup")
	if err != nil {
		t.Fatalf("NewObjectStoreTarget() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := target.Backup(ctx, t.TempDir(), hatBackup.BundleManifest{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Backup() error = %v, want context.Canceled", err)
	}
}

func BenchmarkObjectStoreTargetBackup(b *testing.B) {
	store := newObjectStoreMemory()
	source := b.TempDir()
	writeObjectStoreFile(b, filepath.Join(source, "part", "rows.bin"), bytes.Repeat([]byte("row-value\n"), 1024))
	target, err := hatBackup.NewObjectStoreTarget(store, "bench")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := target.Backup(context.Background(), source, hatBackup.BundleManifest{}); err != nil {
			b.Fatal(err)
		}
	}
}

func writeObjectStoreFile(t testing.TB, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func objectStoreSHA256(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}
