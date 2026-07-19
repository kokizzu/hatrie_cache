package hatriecache

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const (
	benchmarkStorageEntries  = 10_000
	benchmarkStorageChurnOps = 1_000
	benchmarkStorageValueLen = 256
)

func BenchmarkPersistentStorageBackend10k(b *testing.B) {
	for _, backend := range []StorageBackend{StorageBackendLevelDB, StorageBackendPebble} {
		b.Run(storageBackendBenchmarkName(backend), func(b *testing.B) {
			benchmarkPersistentStorageBackend10k(b, backend)
		})
	}
}

func benchmarkPersistentStorageBackend10k(b *testing.B, backend StorageBackend) {
	root := b.TempDir()
	var openDuration time.Duration
	var saveDuration time.Duration
	var churnDuration time.Duration
	var loadDuration time.Duration
	var compactDuration time.Duration
	var closeDuration time.Duration
	var diskBytes int64
	var tableBytes int64
	var walBytes int64
	b.ReportAllocs()
	b.ResetTimer()

	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		path := filepath.Join(root, fmt.Sprintf("run-%06d", iteration))
		source := benchmarkStorageTrie(b, benchmarkStorageEntries, uint64(iteration+1))
		b.StartTimer()

		started := time.Now()
		store, err := OpenPersistentStoreWithFormat(path, backend, StorageFormatBinary)
		openDuration += time.Since(started)
		if err != nil {
			b.Fatalf("OpenPersistentStoreWithFormat() error = %v", err)
		}

		started = time.Now()
		if err := store.Save(source); err != nil {
			b.Fatalf("Save() error = %v", err)
		}
		saveDuration += time.Since(started)

		b.StopTimer()
		churnKeys := benchmarkStorageChurn(source, iteration)
		loaded := CreateHatTrie()
		b.StartTimer()

		started = time.Now()
		if err := store.SaveKeysWithOptions(source, churnKeys, LevelDBSaveOptions{
			CompareBeforeWrite: LevelDBCompareBeforeWriteNever,
		}); err != nil {
			b.Fatalf("SaveKeysWithOptions() error = %v", err)
		}
		churnDuration += time.Since(started)

		started = time.Now()
		count, err := store.Load(loaded)
		loadDuration += time.Since(started)
		if err != nil {
			b.Fatalf("Load() error = %v", err)
		}
		if count != benchmarkStorageEntries || loaded.Size() != benchmarkStorageEntries {
			b.Fatalf("Load() count/size = %d/%d, want %d", count, loaded.Size(), benchmarkStorageEntries)
		}
		if loaded.Exists("key:00000") || !loaded.Exists("new:00000") || loaded.GetBytes("key:00500") == nil {
			b.Fatal("Load() did not preserve delete, insert, and update churn")
		}

		started = time.Now()
		if _, err := store.Compact(LevelDBCompactionOptions{}); err != nil {
			b.Fatalf("Compact() error = %v", err)
		}
		compactDuration += time.Since(started)

		started = time.Now()
		if err := store.Close(); err != nil {
			b.Fatalf("Close() error = %v", err)
		}
		closeDuration += time.Since(started)
		b.StopTimer()

		size, err := directorySizeBytes(path)
		if err != nil {
			b.Fatalf("directorySizeBytes() error = %v", err)
		}
		diskBytes += size
		tables, wal, err := storageEngineFileBytes(path)
		if err != nil {
			b.Fatalf("storageEngineFileBytes() error = %v", err)
		}
		tableBytes += tables
		walBytes += wal
		loaded.Destroy()
		source.Destroy()
		b.StartTimer()
	}

	iterations := float64(b.N)
	b.ReportMetric(float64(openDuration.Nanoseconds())/iterations, "open_ns/op")
	b.ReportMetric(float64(saveDuration.Nanoseconds())/(iterations*benchmarkStorageEntries), "save_ns/key")
	b.ReportMetric(float64(churnDuration.Nanoseconds())/(iterations*benchmarkStorageChurnOps), "churn_ns/op")
	b.ReportMetric(float64(loadDuration.Nanoseconds())/(iterations*benchmarkStorageEntries), "load_ns/key")
	b.ReportMetric(float64(compactDuration.Nanoseconds())/iterations, "compact_ns/op")
	b.ReportMetric(float64(closeDuration.Nanoseconds())/iterations, "close_ns/op")
	b.ReportMetric(float64(diskBytes)/(iterations*benchmarkStorageEntries), "disk_B/key")
	b.ReportMetric(float64(tableBytes)/(iterations*benchmarkStorageEntries), "table_B/key")
	b.ReportMetric(float64(walBytes)/(iterations*benchmarkStorageEntries), "wal_B/key")
}

func storageEngineFileBytes(path string) (tableBytes int64, walBytes int64, err error) {
	err = filepath.WalkDir(path, func(filePath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		info, statErr := os.Stat(filePath)
		if statErr != nil {
			return statErr
		}
		switch filepath.Ext(entry.Name()) {
		case ".ldb", ".sst":
			tableBytes += info.Size()
		case ".log":
			walBytes += info.Size()
		}
		return nil
	})
	return tableBytes, walBytes, err
}

func benchmarkStorageTrie(tb testing.TB, entries int, seed uint64) *HatTrie {
	tb.Helper()
	trie := CreateHatTrie()
	state := seed
	for index := 0; index < entries; index++ {
		value := make([]byte, benchmarkStorageValueLen)
		for offset := range value {
			state ^= state << 13
			state ^= state >> 7
			state ^= state << 17
			value[offset] = byte(state)
		}
		trie.UpsertBytes(fmt.Sprintf("key:%05d", index), value)
	}
	return trie
}

func benchmarkStorageChurn(trie *HatTrie, iteration int) []string {
	keys := make([]string, 0, benchmarkStorageChurnOps)
	for index := 0; index < 500; index++ {
		key := fmt.Sprintf("key:%05d", index+500)
		trie.UpsertBytes(key, []byte(fmt.Sprintf("updated:%06d:%05d", iteration, index)))
		keys = append(keys, key)
	}
	for index := 0; index < 250; index++ {
		key := fmt.Sprintf("key:%05d", index)
		trie.Delete(key)
		keys = append(keys, key)
	}
	for index := 0; index < 250; index++ {
		key := fmt.Sprintf("new:%05d", index)
		trie.UpsertBytes(key, []byte(fmt.Sprintf("inserted:%06d:%05d", iteration, index)))
		keys = append(keys, key)
	}
	return keys
}

func storageBackendBenchmarkName(backend StorageBackend) string {
	switch backend {
	case StorageBackendLevelDB:
		return "LevelDB"
	case StorageBackendPebble:
		return "Pebble"
	default:
		return string(backend)
	}
}
