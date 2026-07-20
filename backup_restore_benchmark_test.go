package hatriecache

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func BenchmarkSinglePassAtomicRestore10k(b *testing.B) {
	keyCount := benchmarkBackupKeys(10_000)
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	for index := 0; index < keyCount; index++ {
		trie.UpsertString("restore:key:"+strconv.Itoa(index), benchmarkBackupValue(index, 256))
	}

	root := b.TempDir()
	snapshotBundle := filepath.Join(root, "snapshot.tar.gz")
	if _, err := CreateBackupBundle(snapshotBundle, trie, nil, BackupBundleOptions{
		Mode:           BackupModeSnapshot,
		SnapshotFormat: DefaultSnapshotFormat,
	}); err != nil {
		b.Fatal(err)
	}
	checkpointBundle := filepath.Join(root, "checkpoint.tar.gz")
	store := openBenchmarkBackupStore(b, trie, "restore-source.pebble")
	if _, err := CreateBackupBundle(checkpointBundle, trie, nil, BackupBundleOptions{
		Mode:            BackupModePebbleCheckpoint,
		PersistentStore: store,
	}); err != nil {
		b.Fatal(err)
	}

	benchmarkRestorePath(b, "Snapshot", snapshotBundle, keyCount)
	benchmarkRestorePath(b, "Checkpoint", checkpointBundle, keyCount)
	repository := filepath.Join(root, "repository")
	if _, err := CreateBackupBundle(repository, trie, nil, BackupBundleOptions{
		Mode:            BackupModePebbleIncremental,
		PersistentStore: store,
		DirtyTracker:    NewLevelDBDirtyTracker(),
	}); err != nil {
		b.Fatal(err)
	}
	benchmarkRepositoryRestorePath(b, repository, keyCount)
}

func benchmarkRestorePath(b *testing.B, name string, bundlePath string, keyCount int) {
	b.Run(name+"/LegacyDoublePass", func(b *testing.B) {
		destinationRoot := b.TempDir()
		info, err := os.Stat(bundlePath)
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		b.ReportAllocs()
		b.ReportMetric(2, "payload_passes/op")
		b.ReportMetric(float64(info.Size()), "bundle_B/op")
		for iteration := 0; iteration < b.N; iteration++ {
			destination := filepath.Join(destinationRoot, "legacy-"+strconv.Itoa(iteration))
			report, err := benchmarkLegacyDoublePassRestore(bundlePath, destination)
			if err != nil || report.RecoveredKeys != keyCount {
				b.Fatalf("legacy restore = %#v/%v", report, err)
			}
		}
	})
	b.Run(name+"/AtomicSinglePass", func(b *testing.B) {
		destinationRoot := b.TempDir()
		info, err := os.Stat(bundlePath)
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		b.ReportAllocs()
		b.ReportMetric(1, "payload_passes/op")
		b.ReportMetric(float64(info.Size()), "bundle_B/op")
		for iteration := 0; iteration < b.N; iteration++ {
			destination := filepath.Join(destinationRoot, "atomic-"+strconv.Itoa(iteration))
			report, err := RestoreBackupBundle(bundlePath, destination, BackupBundleRestoreOptions{})
			if err != nil || report.RecoveredKeys != keyCount {
				b.Fatalf("atomic restore = %#v/%v", report, err)
			}
		}
	})
}

func benchmarkRepositoryRestorePath(b *testing.B, repository string, keyCount int) {
	manifest, err := readBackupRepositoryManifest(repository, "")
	if err != nil {
		b.Fatal(err)
	}
	logicalBytes := backupRepositoryLogicalBytes(manifest.Files)
	b.Run("Repository/LegacyDoubleMaterialize", func(b *testing.B) {
		destinationRoot := b.TempDir()
		b.ResetTimer()
		b.ReportAllocs()
		b.ReportMetric(2, "payload_passes/op")
		b.ReportMetric(float64(logicalBytes), "logical_B/op")
		for iteration := 0; iteration < b.N; iteration++ {
			destination := filepath.Join(destinationRoot, "legacy-"+strconv.Itoa(iteration))
			report, err := benchmarkLegacyDoubleMaterializeRepository(repository, destination)
			if err != nil || report.RecoveredKeys != keyCount {
				b.Fatalf("legacy repository restore = %#v/%v", report, err)
			}
		}
	})
	b.Run("Repository/AtomicSingleMaterialize", func(b *testing.B) {
		destinationRoot := b.TempDir()
		b.ResetTimer()
		b.ReportAllocs()
		b.ReportMetric(1, "payload_passes/op")
		b.ReportMetric(float64(logicalBytes), "logical_B/op")
		for iteration := 0; iteration < b.N; iteration++ {
			destination := filepath.Join(destinationRoot, "atomic-"+strconv.Itoa(iteration))
			report, err := RestoreBackupBundle(repository, destination, BackupBundleRestoreOptions{})
			if err != nil || report.RecoveredKeys != keyCount {
				b.Fatalf("atomic repository restore = %#v/%v", report, err)
			}
		}
	})
}

func benchmarkLegacyDoublePassRestore(bundlePath string, destination string) (BackupDoctorReport, error) {
	manifest, err := readBackupBundleManifest(bundlePath)
	if err != nil {
		return BackupDoctorReport{}, err
	}
	doctor, err := VerifyBackupBundle(bundlePath)
	if err != nil {
		return BackupDoctorReport{}, err
	}
	if err := ensureRestoreDataDir(destination, false); err != nil {
		return BackupDoctorReport{}, err
	}
	if err := extractBackupBundleFiles(bundlePath, destination, manifest.Files); err != nil {
		return BackupDoctorReport{}, err
	}
	return doctor, nil
}

func benchmarkLegacyDoubleMaterializeRepository(repository string, destination string) (BackupDoctorReport, error) {
	doctor, err := VerifyBackupRepository(repository, "")
	if err != nil {
		return BackupDoctorReport{}, err
	}
	manifest, err := readBackupRepositoryManifest(repository, doctor.BackupID)
	if err != nil {
		return BackupDoctorReport{}, err
	}
	if err := ensureRestoreDataDir(destination, false); err != nil {
		return BackupDoctorReport{}, err
	}
	if _, err := materializeBackupRepository(repository, manifest.BackupID, destination); err != nil {
		return BackupDoctorReport{}, err
	}
	return doctor, nil
}
