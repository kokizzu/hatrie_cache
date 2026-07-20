package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	hatriecache "hatrie_cache"
)

func BenchmarkCheckpointReplicaBootstrap10k(b *testing.B) {
	keyCount := checkpointBootstrapBenchmarkKeys(10_000)
	sourceTrie := hatriecache.CreateHatTrie()
	b.Cleanup(sourceTrie.Destroy)
	for index := 0; index < keyCount; index++ {
		sourceTrie.UpsertString("bootstrap:key:"+strconv.Itoa(index), checkpointBootstrapBenchmarkValue(index, 256))
	}
	sourceJournal, err := hatriecache.OpenCommandJournal(filepath.Join(b.TempDir(), "source.journal"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = sourceJournal.Close() })
	if response := sourceJournal.ExecuteCommand(sourceTrie, hatriecache.CacheCommandRequest{Command: "SETSTR", Key: "bootstrap:ready", Value: "yes"}); !response.OK {
		b.Fatal(response.Message)
	}
	snapshotPath := filepath.Join(b.TempDir(), "source.snapshot.hc")
	if err := sourceJournal.SaveSnapshotWithFormat(sourceTrie, snapshotPath, hatriecache.SnapshotFormatGzipBinary); err != nil {
		b.Fatal(err)
	}
	snapshot, err := os.ReadFile(snapshotPath)
	if err != nil {
		b.Fatal(err)
	}
	sourceStore, err := hatriecache.OpenPebbleStore(filepath.Join(b.TempDir(), "source.pebble"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = sourceStore.Close() })
	checkpointPath := filepath.Join(b.TempDir(), "source.checkpoint.tar.gz")
	if _, err := hatriecache.CreateBackupBundle(checkpointPath, sourceTrie, sourceJournal, hatriecache.BackupBundleOptions{
		Mode:            hatriecache.BackupModePebbleCheckpoint,
		PersistentStore: sourceStore,
	}); err != nil {
		b.Fatal(err)
	}
	checkpoint, err := os.ReadFile(checkpointPath)
	if err != nil {
		b.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/journal/snapshot":
			w.Header().Set("Content-Type", "application/vnd.hatrie-cache.snapshot")
			_, _ = w.Write(snapshot)
		case "/api/journal/checkpoint":
			w.Header().Set("Content-Type", "application/vnd.hatrie-cache.pebble-checkpoint")
			_, _ = w.Write(checkpoint)
		default:
			http.NotFound(w, r)
		}
	}))
	b.Cleanup(server.Close)

	b.Run("SnapshotFallback", func(b *testing.B) {
		root := b.TempDir()
		b.ResetTimer()
		b.ReportAllocs()
		b.ReportMetric(float64(len(snapshot)), "wire_B/op")
		for iteration := 0; iteration < b.N; iteration++ {
			if err := benchmarkSnapshotReplicaBootstrap(server.URL, filepath.Join(root, strconv.Itoa(iteration)), keyCount+1); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("PebbleCheckpoint", func(b *testing.B) {
		root := b.TempDir()
		b.ResetTimer()
		b.ReportAllocs()
		b.ReportMetric(float64(len(checkpoint)), "wire_B/op")
		for iteration := 0; iteration < b.N; iteration++ {
			if err := benchmarkCheckpointReplicaBootstrap(server.URL, filepath.Join(root, strconv.Itoa(iteration)), keyCount+1); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func benchmarkSnapshotReplicaBootstrap(source string, root string, expectedKeys int) error {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return err
	}
	trie := hatriecache.CreateHatTrie()
	defer trie.Destroy()
	journal, err := hatriecache.OpenCommandJournal(filepath.Join(root, "commands.journal"))
	if err != nil {
		return err
	}
	defer journal.Close()
	store, err := hatriecache.OpenPebbleStore(filepath.Join(root, "cache.leveldb"))
	if err != nil {
		return err
	}
	defer store.Close()
	snapshotPath := filepath.Join(root, "pull.snapshot.hc")
	metadata, err := hatriecache.PullCommandJournalSnapshot(context.Background(), source, "", http.DefaultClient, snapshotPath)
	if err != nil {
		return err
	}
	if _, err := journal.ReplaceWithSnapshot(trie, snapshotPath); err != nil {
		return err
	}
	if trie.Size() != expectedKeys {
		return os.ErrInvalid
	}
	if err := store.Save(trie); err != nil {
		return err
	}
	return saveJournalPullState(filepath.Join(root, "pull-state.json"), source, metadata.JournalSequence)
}

func benchmarkCheckpointReplicaBootstrap(source string, root string, expectedKeys int) error {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return err
	}
	cfg := config{
		dbPath:                         filepath.Join(root, "cache.leveldb"),
		dbBackend:                      string(hatriecache.StorageBackendAuto),
		dbFormat:                       string(hatriecache.DefaultStorageFormat),
		journalPath:                    filepath.Join(root, "commands.journal"),
		journalFormat:                  string(hatriecache.DefaultCommandJournalFormat),
		journalPullSource:              source,
		journalPullStatePath:           filepath.Join(root, "pull-state.json"),
		journalPullFullSyncFallback:    true,
		journalPullCheckpointBootstrap: true,
	}
	if _, installed, err := bootstrapReplicaCheckpoint(context.Background(), cfg); err != nil || !installed {
		if err != nil {
			return err
		}
		return os.ErrInvalid
	}
	store, err := hatriecache.OpenPersistentStore(cfg.dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	trie := hatriecache.CreateHatTrie()
	defer trie.Destroy()
	if _, err := store.Load(trie); err != nil {
		return err
	}
	if trie.Size() != expectedKeys {
		return os.ErrInvalid
	}
	journal, err := hatriecache.OpenCommandJournal(cfg.journalPath)
	if err != nil {
		return err
	}
	return journal.Close()
}

func checkpointBootstrapBenchmarkKeys(fallback int) int {
	value := strings.TrimSpace(os.Getenv("HATRIE_BACKUP_BENCH_KEYS"))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func checkpointBootstrapBenchmarkValue(index int, size int) string {
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
