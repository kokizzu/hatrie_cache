package hatBackup

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func BenchmarkC240RestoreSingleFile(b *testing.B) {
	target, _ := newC240BenchmarkTarget(b)
	ctx := context.Background()
	restoreRoot := filepath.Join(b.TempDir(), "restore")
	if err := os.MkdirAll(restoreRoot, 0o700); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		destination := filepath.Join(restoreRoot, strconv.Itoa(index))
		if _, err := target.Restore(ctx, destination, false); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkC240AttachReadFile(b *testing.B) {
	target, filePath := newC240BenchmarkTarget(b)
	ctx := context.Background()
	name := filepath.ToSlash(filepath.Base(filePath))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		attachment, err := target.AttachReadOnly(ctx)
		if err != nil {
			b.Fatal(err)
		}
		data, err := attachment.ReadFile(ctx, name)
		if err != nil {
			b.Fatal(err)
		}
		if len(data) == 0 {
			b.Fatal("attached benchmark payload is empty")
		}
	}
}

func BenchmarkC240AttachedReadFile(b *testing.B) {
	target, filePath := newC240BenchmarkTarget(b)
	ctx := context.Background()
	attachment, err := target.AttachReadOnly(ctx)
	if err != nil {
		b.Fatal(err)
	}
	name := filepath.ToSlash(filepath.Base(filePath))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		data, err := attachment.ReadFile(ctx, name)
		if err != nil {
			b.Fatal(err)
		}
		if len(data) == 0 {
			b.Fatal("attached benchmark payload is empty")
		}
	}
}

func BenchmarkC240AttachStreamFile(b *testing.B) {
	target, filePath := newC240BenchmarkTarget(b)
	ctx := context.Background()
	name := filepath.ToSlash(filepath.Base(filePath))
	buffer := make([]byte, 32<<10)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		attachment, err := target.AttachReadOnly(ctx)
		if err != nil {
			b.Fatal(err)
		}
		reader, err := attachment.Open(ctx, name)
		if err != nil {
			b.Fatal(err)
		}
		_, copyErr := io.CopyBuffer(io.Discard, reader, buffer)
		closeErr := reader.Close()
		if copyErr != nil {
			b.Fatal(copyErr)
		}
		if closeErr != nil {
			b.Fatal(closeErr)
		}
	}
}

func BenchmarkC240AttachedStreamFile(b *testing.B) {
	target, filePath := newC240BenchmarkTarget(b)
	ctx := context.Background()
	attachment, err := target.AttachReadOnly(ctx)
	if err != nil {
		b.Fatal(err)
	}
	name := filepath.ToSlash(filepath.Base(filePath))
	buffer := make([]byte, 32<<10)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		reader, err := attachment.Open(ctx, name)
		if err != nil {
			b.Fatal(err)
		}
		_, copyErr := io.CopyBuffer(io.Discard, reader, buffer)
		closeErr := reader.Close()
		if copyErr != nil {
			b.Fatal(copyErr)
		}
		if closeErr != nil {
			b.Fatal(closeErr)
		}
	}
}

func newC240BenchmarkTarget(b *testing.B) (*ObjectStoreTarget, string) {
	b.Helper()
	store := newC240ObjectStore()
	target, err := NewObjectStoreTarget(store, "benchmarks/c240")
	if err != nil {
		b.Fatal(err)
	}
	root := b.TempDir()
	filePath := filepath.Join(root, "rows.bin")
	if err := os.WriteFile(filePath, make([]byte, 64<<10), 0o600); err != nil {
		b.Fatal(err)
	}
	if _, err := target.Backup(context.Background(), root, BundleManifest{BackupID: "c240-benchmark"}); err != nil {
		b.Fatal(err)
	}
	return target, filePath
}
