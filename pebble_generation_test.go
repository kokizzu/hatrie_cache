package hatriecache

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestPebbleStoreFullSaveSwitchesExactGenerations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache")
	store, err := OpenPebbleStore(path)
	if err != nil {
		t.Fatalf("OpenPebbleStore() error = %v", err)
	}
	defer store.Close()

	trie := newTestTrie(t)
	trie.UpsertString("alpha", "one")
	trie.UpsertString("stale", "remove")
	if err := store.Save(trie); err != nil {
		t.Fatalf("Save(generation 1) error = %v", err)
	}
	if store.activeGeneration != 1 {
		t.Fatalf("active generation = %d, want 1", store.activeGeneration)
	}

	trie.UpsertString("alpha", "two")
	trie.UpsertString("new", "value")
	trie.Delete("stale")
	if err := store.Save(trie); err != nil {
		t.Fatalf("Save(generation 2) error = %v", err)
	}
	if store.activeGeneration != 2 {
		t.Fatalf("active generation = %d, want 2", store.activeGeneration)
	}

	restored := newTestTrie(t)
	if count, err := store.Load(restored); err != nil || count != 2 {
		t.Fatalf("Load() = %d/%v, want 2/nil", count, err)
	}
	if restored.GetString("alpha") != "two" || restored.GetString("new") != "value" || restored.Exists("stale") {
		t.Fatalf("restored generation = %#v", restored.Entries(true))
	}
	if got := countPebbleGenerationEntries(t, store, 1); got != 0 {
		t.Fatalf("generation 1 visible entries = %d, want 0", got)
	}
	if got := countPebbleGenerationEntries(t, store, 2); got != 2 {
		t.Fatalf("generation 2 visible entries = %d, want 2", got)
	}
	if got := countPebbleLegacyEntries(t, store); got != 0 {
		t.Fatalf("legacy visible entries = %d, want 0", got)
	}
}

func TestPebbleStoreGenerationSaveCrashBeforeActivationKeepsOldState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache")
	store, err := OpenPebbleStore(path)
	if err != nil {
		t.Fatalf("OpenPebbleStore() error = %v", err)
	}
	trie := newTestTrie(t)
	trie.UpsertString("key", "old")
	if err := store.Save(trie); err != nil {
		t.Fatalf("Save(baseline) error = %v", err)
	}

	injected := errors.New("injected crash before activation")
	store.generationSaveHook = func(stage string) error {
		if stage == pebbleGenerationStageAfterIngest {
			return injected
		}
		return nil
	}
	trie.UpsertString("key", "new")
	if err := store.Save(trie); !errors.Is(err, injected) {
		t.Fatalf("Save(injected) error = %v, want injected", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := OpenPebbleStore(path)
	if err != nil {
		t.Fatalf("OpenPebbleStore(reopen) error = %v", err)
	}
	defer reopened.Close()
	restored := newTestTrie(t)
	if _, err := reopened.Load(restored); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got := restored.GetString("key"); got != "old" {
		t.Fatalf("restored key = %q, want old", got)
	}
	if got := countPebbleGenerationEntries(t, reopened, 2); got != 0 {
		t.Fatalf("orphan generation entries = %d, want startup cleanup", got)
	}
}

func TestPebbleStoreGenerationSaveCrashAfterActivationKeepsNewState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache")
	store, err := OpenPebbleStore(path)
	if err != nil {
		t.Fatalf("OpenPebbleStore() error = %v", err)
	}
	trie := newTestTrie(t)
	trie.UpsertString("key", "old")
	if err := store.Save(trie); err != nil {
		t.Fatalf("Save(baseline) error = %v", err)
	}

	injected := errors.New("injected crash after activation")
	store.generationSaveHook = func(stage string) error {
		if stage == pebbleGenerationStageAfterActivate {
			return injected
		}
		return nil
	}
	trie.UpsertString("key", "new")
	if err := store.Save(trie); !errors.Is(err, injected) {
		t.Fatalf("Save(injected) error = %v, want injected", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := OpenPebbleStore(path)
	if err != nil {
		t.Fatalf("OpenPebbleStore(reopen) error = %v", err)
	}
	defer reopened.Close()
	restored := newTestTrie(t)
	if _, err := reopened.Load(restored); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got := restored.GetString("key"); got != "new" {
		t.Fatalf("restored key = %q, want new", got)
	}
	if got := countPebbleGenerationEntries(t, reopened, 1); got != 0 {
		t.Fatalf("old generation entries = %d, want startup cleanup", got)
	}
}

func TestPebbleStoreGenerationSaveReconcilesConcurrentMutations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache")
	store, err := OpenPebbleStore(path)
	if err != nil {
		t.Fatalf("OpenPebbleStore() error = %v", err)
	}
	defer store.Close()
	trie := newTestTrie(t)
	for index := 0; index < snapshotCaptureScanPageEntries*2; index++ {
		trie.UpsertString(fmt.Sprintf("key:%05d", index), "before")
	}

	firstPage := make(chan struct{})
	release := make(chan struct{})
	var firstPageOnce sync.Once
	trie.snapshotCapturePageHook = func(page int) {
		if page == 1 {
			firstPageOnce.Do(func() {
				close(firstPage)
				<-release
			})
		}
	}
	saved := make(chan error, 1)
	go func() { saved <- store.Save(trie) }()
	<-firstPage
	trie.UpsertString("key:00000", "after")
	trie.Delete("key:00001")
	trie.UpsertString("key:00000:new", "inserted")
	close(release)
	if err := <-saved; err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	restored := newTestTrie(t)
	if _, err := store.Load(restored); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if restored.GetString("key:00000") != "after" || restored.Exists("key:00001") || restored.GetString("key:00000:new") != "inserted" {
		t.Fatalf("concurrent mutation state = %#v", restored.Entries(true)[:3])
	}
}

func TestPebbleStoreGenerationSaveReconcilesConcurrentPartitionMutations(t *testing.T) {
	store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	trie := newTestTrie(t)
	if err := trie.ConfigureLocalPartitions(8); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < snapshotCaptureScanPageEntries*2; index++ {
		trie.UpsertString(fmt.Sprintf("partition-pebble:%05d", index), "before")
	}

	firstPage := make(chan struct{})
	release := make(chan struct{})
	var firstPageOnce sync.Once
	trie.snapshotCapturePageHook = func(page int) {
		if page == 1 {
			firstPageOnce.Do(func() {
				close(firstPage)
				<-release
			})
		}
	}
	saved := make(chan error, 1)
	go func() { saved <- store.Save(trie) }()
	<-firstPage
	trie.UpsertString("partition-pebble:00000", "after")
	trie.Delete("partition-pebble:00001")
	trie.UpsertString("partition-pebble:00000:new", "inserted")
	close(release)
	if err := <-saved; err != nil {
		t.Fatal(err)
	}

	restored := newTestTrie(t)
	if _, err := store.Load(restored); err != nil {
		t.Fatal(err)
	}
	if got := restored.GetString("partition-pebble:00000"); got != "after" {
		t.Fatalf("updated partition value = %q, want after", got)
	}
	if restored.Exists("partition-pebble:00001") {
		t.Fatal("deleted partition value remained in Pebble generation")
	}
	if got := restored.GetString("partition-pebble:00000:new"); got != "inserted" {
		t.Fatalf("inserted partition value = %q, want inserted", got)
	}
}

func TestPebbleStoreGenerationSaveSupportsEmptyJSONAndLazyReferences(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache")
	store, err := OpenPebbleStoreWithFormat(path, StorageFormatJSON)
	if err != nil {
		t.Fatalf("OpenPebbleStoreWithFormat() error = %v", err)
	}
	defer store.Close()
	trie := newTestTrie(t)
	coldValue := strings.Repeat("x", 2048)
	trie.UpsertString("cold", coldValue)
	if err := store.Save(trie); err != nil {
		t.Fatalf("Save(JSON) error = %v", err)
	}

	lazy := newTestTrie(t)
	result, err := store.LoadWithPolicy(lazy, DefaultLevelDBHotLoadPolicy())
	if err != nil || result.ValuesLoaded != 0 {
		t.Fatalf("LoadWithPolicy() = %#v/%v, want lazy", result, err)
	}
	if err := store.Save(lazy); err != nil {
		t.Fatalf("Save(lazy generation) error = %v", err)
	}
	if got := lazy.GetString("cold"); got != coldValue {
		t.Fatalf("lazy value after generation switch = %q", got)
	}

	lazy.Delete("cold")
	if err := store.Save(lazy); err != nil {
		t.Fatalf("Save(empty generation) error = %v", err)
	}
	restored := newTestTrie(t)
	if count, err := store.Load(restored); err != nil || count != 0 {
		t.Fatalf("Load(empty generation) = %d/%v, want 0/nil", count, err)
	}
	matches, err := filepath.Glob(filepath.Join(filepath.Dir(path), filepath.Base(path)+".generation-*.sst"))
	if err != nil || len(matches) != 0 {
		t.Fatalf("generation temporary files = %v/%v, want none", matches, err)
	}
}

func countPebbleGenerationEntries(t *testing.T, store *PebbleStore, generation uint64) int {
	t.Helper()
	return countPebblePrefix(t, store, pebbleGenerationEntryPrefix(generation))
}

func countPebbleLegacyEntries(t *testing.T, store *PebbleStore) int {
	t.Helper()
	return countPebblePrefix(t, store, levelDBEntryPrefix)
}

func countPebblePrefix(t *testing.T, store *PebbleStore, prefix []byte) int {
	t.Helper()
	db, unlock, err := store.lockDB()
	if err != nil {
		t.Fatalf("lockDB() error = %v", err)
	}
	defer unlock()
	iterator, err := db.NewIter(pebblePrefixIterOptions(prefix))
	if err != nil {
		t.Fatalf("NewIter() error = %v", err)
	}
	defer iterator.Close()
	count := 0
	for valid := iterator.First(); valid; valid = iterator.Next() {
		count++
	}
	if err := iterator.Error(); err != nil {
		t.Fatalf("iterator error = %v", err)
	}
	return count
}
