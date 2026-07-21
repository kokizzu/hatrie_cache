package main

import (
	"context"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"

	hatriecache "hatrie_cache"
)

func BenchmarkExistingReplicaRecovery10k(b *testing.B) {
	keyCount := recoveryBenchmarkInt("HATRIE_RECOVERY_BENCH_KEYS", 10000)
	changed := keyCount / 100
	if changed < 1 {
		changed = 1
	}
	sourceTrie := hatriecache.CreateHatTrie()
	b.Cleanup(sourceTrie.Destroy)
	sourceStore, err := hatriecache.OpenPebbleStore(filepath.Join(b.TempDir(), "source.pebble"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = sourceStore.Close() })
	sourceJournal, err := hatriecache.OpenCommandJournal(filepath.Join(b.TempDir(), "source.journal"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = sourceJournal.Close() })
	dirty := hatriecache.NewLevelDBDirtyTracker()
	for index := 0; index < keyCount; index++ {
		key := recoveryBenchmarkKey(index)
		sourceTrie.UpsertString(key, recoveryBenchmarkValue(index, 256))
		dirty.Mark(key)
	}
	if response := sourceJournal.ExecuteCommand(sourceTrie, hatriecache.CacheCommandRequest{Command: "SETSTR", Key: "generation", Value: "base"}); !response.OK {
		b.Fatal(response.Message)
	}
	dirty.Mark("generation")

	root := b.TempDir()
	sourceRepository := filepath.Join(root, "source-repository")
	var wireBytes atomic.Int64
	sourceHandler := hatriecache.NewMonitoringHandler(sourceTrie, hatriecache.MonitoringOptions{
		Journal:                       sourceJournal,
		LevelDBStore:                  sourceStore,
		LevelDBDirtyTracker:           dirty,
		JournalRecoveryRepositoryPath: sourceRepository,
	}).Handler()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sourceHandler.ServeHTTP(&recoveryBenchmarkResponseWriter{ResponseWriter: w, bytes: &wireBytes}, r)
	}))
	b.Cleanup(server.Close)

	baseRepository := filepath.Join(root, "base-repository")
	baseRecovery, err := hatriecache.PullCommandJournalRecovery(context.Background(), server.URL, "", server.Client(), baseRepository, 0)
	if err != nil {
		b.Fatal(err)
	}
	batch := make([]hatriecache.CacheCommandRequest, 0, changed+1)
	for index := 0; index < changed; index++ {
		key := recoveryBenchmarkKey(index)
		batch = append(batch, hatriecache.CacheCommandRequest{Command: "SETSTR", Key: key, Value: recoveryBenchmarkValue(keyCount+index, 256)})
		dirty.Mark(key)
	}
	batch = append(batch, hatriecache.CacheCommandRequest{Command: "SETSTR", Key: "generation", Value: "changed"})
	dirty.Mark("generation")
	if response := sourceJournal.ExecuteCommand(sourceTrie, hatriecache.CacheCommandRequest{Command: "BATCH", Batch: batch}); !response.OK {
		b.Fatal(response.Message)
	}

	for _, test := range []struct {
		name        string
		incremental bool
	}{
		{name: "FullSnapshot"},
		{name: "IncrementalRepository", incremental: true},
	} {
		b.Run(test.name, func(b *testing.B) {
			var transferred int64
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				b.StopTimer()
				iterationRoot := filepath.Join(root, test.name+"-"+strconv.Itoa(iteration))
				if err := os.RemoveAll(iterationRoot); err != nil {
					b.Fatal(err)
				}
				if err := os.MkdirAll(iterationRoot, 0o700); err != nil {
					b.Fatal(err)
				}
				targetTrie := hatriecache.CreateHatTrie()
				for index := 0; index < keyCount; index++ {
					targetTrie.UpsertString(recoveryBenchmarkKey(index), recoveryBenchmarkValue(index, 256))
				}
				targetTrie.UpsertString("generation", "base")
				targetTrie.UpsertString("stale", "remove")
				targetStore, err := hatriecache.OpenPebbleStore(filepath.Join(iterationRoot, "target.pebble"))
				if err != nil {
					b.Fatal(err)
				}
				if err := targetStore.Save(targetTrie); err != nil {
					b.Fatal(err)
				}
				journalPath := filepath.Join(iterationRoot, "target.journal")
				if err := hatriecache.InstallCommandJournalCheckpoint(journalPath, hatriecache.DefaultCommandJournalFormat, baseRecovery.Manifest.JournalSequence); err != nil {
					b.Fatal(err)
				}
				targetJournal, err := hatriecache.OpenCommandJournal(journalPath)
				if err != nil {
					b.Fatal(err)
				}
				localRepository := filepath.Join(iterationRoot, "recovery-repository")
				if test.incremental {
					if err := linkRecoveryBenchmarkTree(baseRepository, localRepository); err != nil {
						b.Fatal(err)
					}
				}
				wireBytes.Store(0)
				b.StartTimer()
				if test.incremental {
					_, err = pullJournalIncrementalRecovery(context.Background(), targetTrie, targetJournal, journalPullerConfig{
						Source:                  server.URL,
						RecoveryRepositoryPath:  localRepository,
						RecoveryStageParent:     filepath.Dir(targetStore.Path()),
						StorageFormat:           hatriecache.DefaultStorageFormat,
						Client:                  server.Client(),
						AdoptRecoveryCheckpoint: targetStore.AdoptCheckpoint,
					}, baseRecovery.Manifest.JournalSequence)
				} else {
					snapshotPath := filepath.Join(iterationRoot, "recovery.snapshot")
					var metadata hatriecache.SnapshotMetadata
					metadata, err = hatriecache.PullCommandJournalSnapshot(context.Background(), server.URL, "", server.Client(), snapshotPath, baseRecovery.Manifest.JournalSequence)
					if err == nil {
						_, err = targetJournal.ReplaceWithSnapshot(targetTrie, snapshotPath)
					}
					if err == nil && metadata.JournalSequence != targetJournal.Sequence() {
						err = fmt.Errorf("snapshot sequence mismatch")
					}
					if err == nil {
						err = targetStore.Save(targetTrie)
					}
				}
				b.StopTimer()
				transferred += wireBytes.Load()
				if err != nil {
					b.Fatal(err)
				}
				if targetTrie.Exists("stale") || targetTrie.GetString("generation") != "changed" || targetTrie.GetString(recoveryBenchmarkKey(0)) != recoveryBenchmarkValue(keyCount, 256) {
					b.Fatal("recovered target state mismatch")
				}
				if err := targetJournal.Close(); err != nil {
					b.Fatal(err)
				}
				if err := targetStore.Close(); err != nil {
					b.Fatal(err)
				}
				targetTrie.Destroy()
				b.StartTimer()
			}
			b.StopTimer()
			b.ReportMetric(float64(keyCount), "keys/op")
			b.ReportMetric(float64(changed), "changed_keys/op")
			b.ReportMetric(float64(transferred)/float64(b.N), "wire_B/op")
		})
	}
}

type recoveryBenchmarkResponseWriter struct {
	http.ResponseWriter
	bytes *atomic.Int64
}

func (writer *recoveryBenchmarkResponseWriter) Write(data []byte) (int, error) {
	written, err := writer.ResponseWriter.Write(data)
	writer.bytes.Add(int64(written))
	return written, err
}

func recoveryBenchmarkKey(index int) string {
	return fmt.Sprintf("recovery:%09d", index)
}

func recoveryBenchmarkValue(index int, size int) string {
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

func recoveryBenchmarkInt(name string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(name))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func linkRecoveryBenchmarkTree(source string, destination string) error {
	return filepath.WalkDir(source, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o700)
		}
		return os.Link(path, target)
	})
}
