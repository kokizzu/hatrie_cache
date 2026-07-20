package hatriecache

import (
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
)

var backupBenchmarkMutationSequence atomic.Uint64

func BenchmarkIncrementalBackupRepository10k(b *testing.B) {
	keyCount := benchmarkBackupKeys(10_000)
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	for index := 0; index < keyCount; index++ {
		trie.UpsertString("backup:key:"+strconv.Itoa(index), benchmarkBackupValue(index, 256))
	}

	b.Run("FullBase", func(b *testing.B) {
		for iteration := 0; iteration < b.N; iteration++ {
			b.StopTimer()
			root := b.TempDir()
			store, err := OpenPebbleStore(filepath.Join(root, "live.pebble"))
			if err != nil {
				b.Fatal(err)
			}
			tracker := NewLevelDBDirtyTracker()
			b.StartTimer()
			manifest, err := CreateBackupBundle(filepath.Join(root, "repository"), trie, nil, BackupBundleOptions{
				Mode:            BackupModePebbleIncremental,
				PersistentStore: store,
				DirtyTracker:    tracker,
			})
			b.StopTimer()
			if closeErr := store.Close(); err == nil {
				err = closeErr
			}
			if err != nil {
				b.Fatal(err)
			}
			writtenBytes := benchmarkBackupRepositoryWrittenBytes(b, filepath.Join(root, "repository"), manifest)
			reportIncrementalBackupMetrics(b, manifest, keyCount, writtenBytes)
			b.StartTimer()
		}
	})

	b.Run("FullCheckpoint1Percent", func(b *testing.B) {
		root := b.TempDir()
		store := openBenchmarkBackupStore(b, trie, "full-checkpoint.pebble")
		changed := benchmarkChangedKeys(keyCount)
		bundlePath := filepath.Join(root, "backup.tar.gz")
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			b.StopTimer()
			updateBenchmarkBackupKeys(trie, nil, keyCount, changed, backupBenchmarkMutationSequence.Add(1))
			_ = os.Remove(bundlePath)
			b.StartTimer()
			manifest, err := CreateBackupBundle(bundlePath, trie, nil, BackupBundleOptions{
				Mode:            BackupModePebbleCheckpoint,
				PersistentStore: store,
			})
			if err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			info, err := os.Stat(bundlePath)
			if err != nil {
				b.Fatal(err)
			}
			reportIncrementalBackupMetrics(b, manifest, keyCount, info.Size())
			b.StartTimer()
		}
	})

	b.Run("IncrementalRepository1Percent", func(b *testing.B) {
		root := b.TempDir()
		store, err := OpenPebbleStore(filepath.Join(root, "live.pebble"))
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = store.Close() })
		tracker := NewLevelDBDirtyTracker()
		repository := filepath.Join(root, "repository")
		if _, err := CreateBackupBundle(repository, trie, nil, BackupBundleOptions{
			Mode:            BackupModePebbleIncremental,
			PersistentStore: store,
			DirtyTracker:    tracker,
		}); err != nil {
			b.Fatal(err)
		}
		changed := benchmarkChangedKeys(keyCount)
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			b.StopTimer()
			updateBenchmarkBackupKeys(trie, tracker, keyCount, changed, backupBenchmarkMutationSequence.Add(1))
			b.StartTimer()
			manifest, err := CreateBackupBundle(repository, trie, nil, BackupBundleOptions{
				Mode:            BackupModePebbleIncremental,
				PersistentStore: store,
				DirtyTracker:    tracker,
			})
			if err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			writtenBytes := benchmarkBackupRepositoryWrittenBytes(b, repository, manifest)
			reportIncrementalBackupMetrics(b, manifest, keyCount, writtenBytes)
			b.StartTimer()
		}
	})
}

func benchmarkChangedKeys(keyCount int) int {
	changed := keyCount / 100
	if changed < 1 {
		return 1
	}
	return changed
}

func updateBenchmarkBackupKeys(trie *HatTrie, tracker *LevelDBDirtyTracker, keyCount int, changed int, generation uint64) {
	for index := 0; index < changed; index++ {
		key := "backup:key:" + strconv.Itoa(index)
		trie.UpsertString(key, benchmarkBackupValue(int(generation)*keyCount+index, 256))
		tracker.Mark(key)
	}
}

func reportIncrementalBackupMetrics(b *testing.B, manifest BackupBundleManifest, keyCount int, writtenBytes int64) {
	b.Helper()
	logicalBytes := backupRepositoryLogicalBytes(manifest.Files)
	b.ReportMetric(float64(keyCount), "keys/op")
	b.ReportMetric(float64(writtenBytes), "written_B/op")
	b.ReportMetric(float64(logicalBytes), "logical_B/op")
	if logicalBytes > 0 {
		b.ReportMetric(float64(manifest.ReusedObjectBytes)/float64(logicalBytes)*100, "reuse_percent")
	}
}

func benchmarkBackupRepositoryWrittenBytes(b *testing.B, repository string, manifest BackupBundleManifest) int64 {
	b.Helper()
	writtenBytes := manifest.NewObjectBytes
	for _, path := range []string{
		filepath.Join(repository, backupRepositoryManifestsPath, manifest.BackupID+".json"),
		filepath.Join(repository, backupRepositoryLatestPath),
	} {
		info, err := os.Stat(path)
		if err != nil {
			b.Fatal(err)
		}
		writtenBytes += info.Size()
	}
	if manifest.ParentBackupID == "" {
		info, err := os.Stat(filepath.Join(repository, backupRepositoryDescriptorPath))
		if err != nil {
			b.Fatal(err)
		}
		writtenBytes += info.Size()
	}
	return writtenBytes
}
