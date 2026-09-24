package hatBackup

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkC241WholeFileIncremental(b *testing.B) {
	benchmarkC241Incremental(b, 0)
}

func BenchmarkC241ChunkedIncremental(b *testing.B) {
	benchmarkC241Incremental(b, 64<<10)
}

func benchmarkC241Incremental(b *testing.B, chunkSize int) {
	const fileSize = 1 << 20
	const benchmarkChunkSize = 64 << 10
	ctx := context.Background()
	baseData := make([]byte, fileSize)
	for index := range baseData {
		baseData[index] = byte(index / benchmarkChunkSize)
	}
	changedData := append([]byte(nil), baseData...)
	changedData[fileSize/2+7] ^= 1
	b.SetBytes(fileSize)
	b.ReportAllocs()

	var totalPayload int64
	var totalNewObjects int64
	var totalReusedObjects int64
	for index := 0; index < b.N; index++ {
		b.StopTimer()
		root := b.TempDir()
		filePath := filepath.Join(root, "part", "rows.bin")
		if err := os.MkdirAll(filepath.Dir(filePath), 0o700); err != nil {
			b.Fatal(err)
		}
		if err := os.WriteFile(filePath, baseData, 0o600); err != nil {
			b.Fatal(err)
		}
		store := newC241ObjectStore()
		target, err := NewObjectStoreTargetWithOptions(store, "backups/c241-benchmark", ObjectStoreTargetOptions{
			Layout:    ObjectStoreLayoutContentAddressed,
			ChunkSize: chunkSize,
		})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := target.Backup(ctx, root, c241Manifest("base", "", false)); err != nil {
			b.Fatal(err)
		}
		if err := os.WriteFile(filePath, changedData, 0o600); err != nil {
			b.Fatal(err)
		}
		store.putBytes = 0
		store.putKeys = nil
		b.StartTimer()
		manifest, err := target.Backup(ctx, root, c241Manifest("next", "base", true))
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		totalPayload += store.putBytes
		totalNewObjects += int64(manifest.NewObjects)
		totalReusedObjects += int64(manifest.ReusedObjects)
	}
	if b.N > 0 {
		b.ReportMetric(float64(totalPayload)/float64(b.N), "payload-bytes/op")
		b.ReportMetric(float64(totalNewObjects)/float64(b.N), "new-objects/op")
		b.ReportMetric(float64(totalReusedObjects)/float64(b.N), "reused-objects/op")
	}
}
