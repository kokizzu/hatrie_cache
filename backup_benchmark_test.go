package hatriecache

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func BenchmarkPebbleCheckpointBackup10k(b *testing.B) {
	keyCount := benchmarkBackupKeys(10_000)
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	for index := 0; index < keyCount; index++ {
		trie.UpsertString("backup:key:"+strconv.Itoa(index), benchmarkBackupValue(index, 256))
	}

	benchmarkBackupCreate(b, trie, keyCount, "Snapshot", func(*testing.B) BackupBundleOptions {
		return BackupBundleOptions{Mode: BackupModeSnapshot, SnapshotFormat: DefaultSnapshotFormat}
	})
	benchmarkBackupRestore(b, trie, keyCount, "Snapshot", func(*testing.B) BackupBundleOptions {
		return BackupBundleOptions{Mode: BackupModeSnapshot, SnapshotFormat: DefaultSnapshotFormat}
	})
	benchmarkBackupCreate(b, trie, keyCount, "Checkpoint", func(b *testing.B) BackupBundleOptions {
		return BackupBundleOptions{Mode: BackupModePebbleCheckpoint, PersistentStore: openBenchmarkBackupStore(b, trie, "create.pebble")}
	})
	benchmarkBackupRestore(b, trie, keyCount, "Checkpoint", func(b *testing.B) BackupBundleOptions {
		return BackupBundleOptions{Mode: BackupModePebbleCheckpoint, PersistentStore: openBenchmarkBackupStore(b, trie, "restore.pebble")}
	})
}

func benchmarkBackupCreate(b *testing.B, trie *HatTrie, keyCount int, name string, options func(*testing.B) BackupBundleOptions) {
	b.Run("Create/"+name, func(b *testing.B) {
		path := filepath.Join(b.TempDir(), "backup.tar.gz")
		fixture := options(b)
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			_ = os.Remove(path)
			if _, err := CreateBackupBundle(path, trie, nil, fixture); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		reportBackupBundleMetrics(b, path, keyCount)
	})
}

func benchmarkBackupRestore(b *testing.B, trie *HatTrie, keyCount int, name string, options func(*testing.B) BackupBundleOptions) {
	b.Run("Restore/"+name, func(b *testing.B) {
		root := b.TempDir()
		path := filepath.Join(root, "backup.tar.gz")
		fixture := options(b)
		if _, err := CreateBackupBundle(path, trie, nil, fixture); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			destination := filepath.Join(root, "restore-"+strconv.Itoa(iteration))
			report, err := RestoreBackupBundle(path, destination, BackupBundleRestoreOptions{})
			if err != nil || report.RecoveredKeys != keyCount {
				b.Fatalf("RestoreBackupBundle() = %#v/%v", report, err)
			}
		}
		b.StopTimer()
		reportBackupBundleMetrics(b, path, keyCount)
	})
}

func openBenchmarkBackupStore(b *testing.B, trie *HatTrie, name string) *PebbleStore {
	b.Helper()
	store, err := OpenPebbleStore(filepath.Join(b.TempDir(), name))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close() })
	if err := store.Save(trie); err != nil {
		b.Fatal(err)
	}
	return store
}

func reportBackupBundleMetrics(b *testing.B, path string, keyCount int) {
	b.Helper()
	info, err := os.Stat(path)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(info.Size())/float64(keyCount), "bundle_B/key")
	b.ReportMetric(float64(keyCount), "keys/op")
}

func benchmarkBackupValue(index int, size int) string {
	const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_"
	state := uint64(index+1) * 0x9e3779b97f4a7c15
	value := make([]byte, size)
	for position := range value {
		state ^= state >> 12
		state ^= state << 25
		state ^= state >> 27
		value[position] = alphabet[(state*0x2545f4914f6cdd1d)>>58]
	}
	return string(value)
}

func benchmarkBackupKeys(fallback int) int {
	if value := strings.TrimSpace(os.Getenv("HATRIE_BACKUP_BENCH_KEYS")); value != "" {
		parsed, err := strconv.Atoi(value)
		if err == nil && parsed > 0 {
			return parsed
		}
	}
	return fallback
}
