package main

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	hatriecache "hatrie_cache"
)

func TestStartLevelDBSaverDoesNotRewriteCleanLoadedStore(t *testing.T) {
	store, trie := preparedStartupPersistenceStore(t, 128)
	defer store.Close()
	defer trie.Destroy()

	generation := store.ActiveGeneration()
	stop := startLevelDBSaver(context.Background(), trie, store, hatriecache.NewLevelDBDirtyTracker(), time.Hour, hatriecache.LevelDBSaveOptions{}, nil)
	deadline := time.Now().Add(500 * time.Millisecond)
	for store.ActiveGeneration() == generation && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	stop()
	if got := store.ActiveGeneration(); got != generation {
		t.Fatalf("startup saver active generation = %d, want unchanged %d", got, generation)
	}
}

func BenchmarkStartupPersistence10k(b *testing.B) {
	const keys = 10_000
	root := b.TempDir()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		store, trie := preparedStartupPersistenceStoreAtPath(b, filepath.Join(root, fmt.Sprintf("run-%06d", iteration)), keys)
		tracker := hatriecache.NewLevelDBDirtyTracker()
		b.StartTimer()
		if err := store.SaveDirtyWithJournalSequence(trie, tracker, hatriecache.LevelDBSaveOptions{}, 0); err != nil {
			b.Fatalf("startup delta Save() error = %v", err)
		}
		b.StopTimer()
		if err := store.Close(); err != nil {
			b.Fatalf("Close() error = %v", err)
		}
		trie.Destroy()
		b.StartTimer()
	}
}

type testOrBenchmark interface {
	Helper()
	Fatalf(string, ...interface{})
	TempDir() string
}

func preparedStartupPersistenceStore(tb testOrBenchmark, keys int) (*hatriecache.PebbleStore, *hatriecache.HatTrie) {
	tb.Helper()
	return preparedStartupPersistenceStoreAtPath(tb, filepath.Join(tb.TempDir(), "cache"), keys)
}

func preparedStartupPersistenceStoreAtPath(tb testOrBenchmark, path string, keys int) (*hatriecache.PebbleStore, *hatriecache.HatTrie) {
	tb.Helper()
	store, err := hatriecache.OpenPebbleStore(path)
	if err != nil {
		tb.Fatalf("OpenPebbleStore() error = %v", err)
	}
	source := hatriecache.CreateHatTrie()
	for index := 0; index < keys; index++ {
		source.UpsertString(fmt.Sprintf("startup:%08d", index), "persistent-value")
	}
	if err := store.SaveWithJournalSequence(source, 0); err != nil {
		source.Destroy()
		_ = store.Close()
		tb.Fatalf("initial Save() error = %v", err)
	}
	source.Destroy()
	loaded := hatriecache.CreateHatTrie()
	if count, err := store.Load(loaded); err != nil || count != keys {
		loaded.Destroy()
		_ = store.Close()
		tb.Fatalf("Load() = %d/%v, want %d/nil", count, err, keys)
	}
	return store, loaded
}
