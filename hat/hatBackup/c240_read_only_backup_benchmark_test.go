package hatBackup

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
)

const c240BenchmarkPayloadSize = 64 << 10

func BenchmarkC240ReadOnlyOpen(b *testing.B) {
	target, _, _, _ := setupC240BenchmarkTarget(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		view, err := target.OpenReadOnly(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		if len(view.Files()) != 1 {
			b.Fatal("read-only view lost the manifest file")
		}
	}
}

func BenchmarkC240ReadOnlyReadFile(b *testing.B) {
	target, _, manifest, _ := setupC240BenchmarkTarget(b)
	view, err := target.OpenReadOnly(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(c240BenchmarkPayloadSize)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := view.ReadFile(context.Background(), manifest.Files[0].Path)
		if err != nil {
			b.Fatal(err)
		}
		if len(data) != c240BenchmarkPayloadSize {
			b.Fatalf("ReadFile() returned %d bytes, want %d", len(data), c240BenchmarkPayloadSize)
		}
	}
}

func BenchmarkC240ReadOnlyStreamFile(b *testing.B) {
	target, _, manifest, _ := setupC240BenchmarkTarget(b)
	view, err := target.OpenReadOnly(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(c240BenchmarkPayloadSize)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, err := view.OpenFile(context.Background(), manifest.Files[0].Path)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, reader); err != nil {
			b.Fatal(err)
		}
		if err := reader.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkC240RestoreReference(b *testing.B) {
	target, _, manifest, _ := setupC240BenchmarkTarget(b)
	destination := filepath.Join(b.TempDir(), "restore")
	b.SetBytes(c240BenchmarkPayloadSize)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := target.Restore(context.Background(), destination, true); err != nil {
			b.Fatal(err)
		}
		if manifest.BackupID == "never" {
			b.Fatal("unreachable")
		}
	}
}

func setupC240BenchmarkTarget(b *testing.B) (*ObjectStoreTarget, *c240MemoryObjectStore, BundleManifest, string) {
	b.Helper()
	store := newC240MemoryObjectStore()
	target, err := NewObjectStoreTarget(store, "backup")
	if err != nil {
		b.Fatal(err)
	}
	root := b.TempDir()
	payload := bytes.Repeat([]byte("x"), c240BenchmarkPayloadSize)
	path := filepath.Join(root, "data", "value")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		b.Fatal(err)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		b.Fatal(err)
	}
	manifest, err := target.Backup(context.Background(), root, BundleManifest{BackupID: "c240-benchmark"})
	if err != nil {
		b.Fatal(err)
	}
	return target, store, manifest, root
}
