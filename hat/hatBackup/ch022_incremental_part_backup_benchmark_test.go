package hatBackup

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func BenchmarkCH022ObjectStoreIncrementalBackup(b *testing.B) {
	root := b.TempDir()
	for index := 0; index < 32; index++ {
		writeCH022BenchmarkFile(b, filepath.Join(root, fmt.Sprintf("part-%02d", index), "rows.bin"), strings.Repeat(fmt.Sprintf("%02d", index), 16384))
	}
	b.Run("PathFull", func(b *testing.B) {
		store := newCH022ObjectStore()
		target, err := NewObjectStoreTargetWithOptions(store, "path", ObjectStoreTargetOptions{Layout: ObjectStoreLayoutPath})
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := target.Backup(context.Background(), root, BundleManifest{Mode: ModePebbleIncremental, BackupID: "path"}); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(store.payloadBytes)/float64(b.N), "payload_bytes/op")
		b.ReportMetric(float64(store.payloadPutCount())/float64(b.N), "payload_puts/op")
	})
	b.Run("ContentAddressedUnchanged", func(b *testing.B) {
		store := newCH022ObjectStore()
		target, err := NewObjectStoreTarget(store, "content")
		if err != nil {
			b.Fatal(err)
		}
		if _, err := target.Backup(context.Background(), root, BundleManifest{Mode: ModePebbleIncremental, BackupID: "base"}); err != nil {
			b.Fatal(err)
		}
		store.payloadBytes = 0
		store.putKeys = nil
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := target.Backup(context.Background(), root, BundleManifest{Mode: ModePebbleIncremental, BackupID: "next", ParentBackupID: "base", Incremental: true}); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(store.payloadBytes)/float64(b.N), "payload_bytes/op")
		b.ReportMetric(float64(store.payloadPutCount())/float64(b.N), "payload_puts/op")
	})
	b.Run("ContentAddressedNew", func(b *testing.B) {
		store := newCH022ObjectStore()
		store.forceMissing = true
		target, err := NewObjectStoreTarget(store, "content")
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := target.Backup(context.Background(), root, BundleManifest{Mode: ModePebbleIncremental, BackupID: "new"}); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(store.payloadBytes)/float64(b.N), "payload_bytes/op")
		b.ReportMetric(float64(store.payloadPutCount())/float64(b.N), "payload_puts/op")
	})
}

func writeCH022BenchmarkFile(b testing.TB, path, payload string) {
	b.Helper()
	writeCH022File(b, path, []byte(payload))
}
