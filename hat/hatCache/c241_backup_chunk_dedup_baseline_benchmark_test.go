package hatCache

import (
	"context"
	"testing"
)

func BenchmarkC241BackupRepositoryWholeFileBaseline(b *testing.B) {
	const payloadSize = 8 << 20
	baseData := c241Payload(payloadSize, 0x21)
	changedData := append([]byte(nil), baseData...)
	for index := (payloadSize / 2) - 4096; index < (payloadSize/2)-4032; index++ {
		changedData[index] ^= 0x7f
	}

	root := b.TempDir()
	if err := ensureBackupRepository(root); err != nil {
		b.Fatal(err)
	}
	base := BackupBundleManifest{
		Version: BackupBundleVersion,
		Files:   []BackupBundleFile{c241BackupFile("cache.leveldb/large.sst", baseData)},
	}
	if err := storeBackupRepositoryObjects(context.Background(), root, &base, []backupBundlePayloadFile{{name: base.Files[0].Path, data: baseData}}, BackupRepositoryChunkingDisabled); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(changedData)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		changed := BackupBundleManifest{
			Version: BackupBundleVersion,
			Files:   []BackupBundleFile{c241BackupFile("cache.leveldb/large.sst", changedData)},
		}
		if err := storeBackupRepositoryObjects(context.Background(), root, &changed, []backupBundlePayloadFile{{name: changed.Files[0].Path, data: changedData}}, BackupRepositoryChunkingDisabled); err != nil {
			b.Fatal(err)
		}
		if index == 0 {
			b.ReportMetric(float64(changed.NewObjectBytes), "new-object-bytes")
			b.ReportMetric(float64(changed.NewObjects), "new-objects")
		}
	}
}

func BenchmarkC241BackupRepositoryChunked(b *testing.B) {
	const payloadSize = 8 << 20
	baseData := c241Payload(payloadSize, 0x21)
	changedData := append([]byte(nil), baseData...)
	for index := (payloadSize / 2) - 4096; index < (payloadSize/2)-4032; index++ {
		changedData[index] ^= 0x7f
	}

	root := b.TempDir()
	if err := ensureBackupRepository(root); err != nil {
		b.Fatal(err)
	}
	base := BackupBundleManifest{
		Version: BackupBundleVersion,
		Files:   []BackupBundleFile{c241BackupFile("cache.leveldb/large.sst", baseData)},
	}
	if err := storeBackupRepositoryObjects(context.Background(), root, &base, []backupBundlePayloadFile{{name: base.Files[0].Path, data: baseData}}, DefaultBackupRepositoryChunkSize); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(changedData)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		changed := BackupBundleManifest{
			Version: BackupBundleVersion,
			Files:   []BackupBundleFile{c241BackupFile("cache.leveldb/large.sst", changedData)},
		}
		if err := storeBackupRepositoryObjects(context.Background(), root, &changed, []backupBundlePayloadFile{{name: changed.Files[0].Path, data: changedData}}, DefaultBackupRepositoryChunkSize); err != nil {
			b.Fatal(err)
		}
		if index == 0 {
			b.ReportMetric(float64(changed.NewObjectBytes), "new-object-bytes")
			b.ReportMetric(float64(changed.NewObjects), "new-objects")
			b.ReportMetric(float64(changed.ReusedObjectBytes), "reused-object-bytes")
		}
	}
}
