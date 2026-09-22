package hatCache

import (
	"path/filepath"
	"strconv"
	"testing"
)

func BenchmarkC241IncrementalBackupRepositoryChunkDedup(b *testing.B) {
	for _, test := range []struct {
		name      string
		chunkSize int
	}{
		{name: "Chunked1MiB", chunkSize: DefaultBackupRepositoryChunkSize},
		{name: "WholeFileControl", chunkSize: backupRepositoryMaxChunkSize},
	} {
		b.Run(test.name, func(b *testing.B) {
			const keyCount = 10_000
			trie := CreateHatTrie()
			b.Cleanup(trie.Destroy)
			for index := 0; index < keyCount; index++ {
				trie.UpsertString("backup:key:"+strconv.Itoa(index), benchmarkBackupValue(index, 256))
			}
			root := b.TempDir()
			store, err := OpenPebbleStore(filepath.Join(root, "live.pebble"))
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = store.Close() })
			tracker := NewLevelDBDirtyTracker()
			repository := filepath.Join(root, "repository")
			options := BackupBundleOptions{
				Mode:                BackupModePebbleIncremental,
				PersistentStore:     store,
				DirtyTracker:        tracker,
				RepositoryChunkSize: test.chunkSize,
			}
			if _, err := CreateBackupBundle(repository, trie, nil, options); err != nil {
				b.Fatal(err)
			}
			changed := benchmarkChangedKeys(keyCount)
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				b.StopTimer()
				updateBenchmarkBackupKeys(trie, tracker, keyCount, changed, uint64(iteration+1))
				b.StartTimer()
				manifest, err := CreateBackupBundle(repository, trie, nil, options)
				if err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				writtenBytes := benchmarkBackupRepositoryWrittenBytes(b, repository, manifest)
				logicalBytes := backupRepositoryLogicalBytes(manifest.Files)
				b.ReportMetric(float64(writtenBytes), "written_B/op")
				b.ReportMetric(float64(manifest.NewObjectBytes), "new_object_B/op")
				b.ReportMetric(float64(manifest.ReusedObjectBytes), "reused_object_B/op")
				b.ReportMetric(float64(logicalBytes), "logical_B/op")
				b.StartTimer()
			}
		})
	}
}
