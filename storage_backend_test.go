package hatriecache

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func TestPersistentStorageBackendsShareSaveLoadAndChurnContract(t *testing.T) {
	for _, backend := range []StorageBackend{StorageBackendLevelDB, StorageBackendPebble} {
		t.Run(string(backend), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "cache")
			store, err := OpenPersistentStoreWithFormat(path, backend, StorageFormatBinary)
			if err != nil {
				t.Fatalf("OpenPersistentStoreWithFormat() error = %v", err)
			}
			source := newTestTrie(t)
			source.UpsertString("name", "ivi")
			source.UpsertCounter("count", 40)
			source.UpsertBytes("blob", []byte("payload"))
			if err := store.Save(source); err != nil {
				t.Fatalf("Save(initial) error = %v", err)
			}

			source.UpsertString("name", "updated")
			source.UpsertCounter("new", 7)
			if !source.Delete("count") {
				t.Fatal("Delete(count) = false")
			}
			if err := store.SaveKeysWithOptions(source, []string{"name", "new", "count"}, LevelDBSaveOptions{
				CompareBeforeWrite: LevelDBCompareBeforeWriteAlways,
			}); err != nil {
				t.Fatalf("SaveKeysWithOptions(churn) error = %v", err)
			}
			result, err := store.Compact(LevelDBCompactionOptions{})
			if err != nil {
				t.Fatalf("Compact() error = %v", err)
			}
			if result.Store != string(backend) || result.SizeBytesBefore <= 0 || result.SizeBytesAfter <= 0 {
				t.Fatalf("Compact() result = %#v, want %s size metadata", result, backend)
			}
			if err := store.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}

			reopened, err := OpenPersistentStoreWithFormat(path, backend, StorageFormatBinary)
			if err != nil {
				t.Fatalf("OpenPersistentStoreWithFormat(reopen) error = %v", err)
			}
			defer reopened.Close()
			restored := newTestTrie(t)
			loaded, err := reopened.Load(restored)
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if loaded != 3 || restored.GetString("name") != "updated" || restored.GetCounter("new") != 7 || restored.GetBytes("blob") == nil || restored.Exists("count") {
				t.Fatalf("restored state = count=%d name=%q new=%d blob=%q count_exists=%v", loaded, restored.GetString("name"), restored.GetCounter("new"), restored.GetBytes("blob"), restored.Exists("count"))
			}
		})
	}
}

func TestPersistentStorageBackendsCommitAppliedJournalSequenceWithState(t *testing.T) {
	for _, backend := range []StorageBackend{StorageBackendLevelDB, StorageBackendPebble} {
		t.Run(string(backend), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "cache")
			store, err := OpenPersistentStoreWithFormat(path, backend, StorageFormatBinary)
			if err != nil {
				t.Fatalf("OpenPersistentStoreWithFormat() error = %v", err)
			}
			trie := newTestTrie(t)
			trie.UpsertCounter("counter", 1)
			if err := store.SaveWithJournalSequence(trie, 41); err != nil {
				t.Fatalf("SaveWithJournalSequence() error = %v", err)
			}
			if sequence, ok, err := store.AppliedJournalSequence(); err != nil || !ok || sequence != 41 {
				t.Fatalf("AppliedJournalSequence() = %d/%v/%v, want 41/true/nil", sequence, ok, err)
			}

			tracker := NewLevelDBDirtyTracker()
			trie.IncrementCounter("counter", 1)
			tracker.Mark("counter")
			if err := store.SaveDirtyWithJournalSequence(trie, tracker, LevelDBSaveOptions{}, 42); err != nil {
				t.Fatalf("SaveDirtyWithJournalSequence() error = %v", err)
			}
			if tracker.Pending() != 0 {
				t.Fatalf("dirty tracker pending = %d, want 0", tracker.Pending())
			}
			if err := store.SaveDirtyWithJournalSequence(trie, tracker, LevelDBSaveOptions{}, 43); err != nil {
				t.Fatalf("SaveDirtyWithJournalSequence(metadata only) error = %v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}

			reopened, err := OpenPersistentStoreWithFormat(path, backend, StorageFormatBinary)
			if err != nil {
				t.Fatalf("OpenPersistentStoreWithFormat(reopen) error = %v", err)
			}
			defer reopened.Close()
			if sequence, ok, err := reopened.AppliedJournalSequence(); err != nil || !ok || sequence != 43 {
				t.Fatalf("reopened AppliedJournalSequence() = %d/%v/%v, want 43/true/nil", sequence, ok, err)
			}
			restored := newTestTrie(t)
			if count, err := reopened.Load(restored); err != nil || count != 1 || restored.GetCounter("counter") != 2 {
				t.Fatalf("reopened Load() = %d/%v counter=%d, want 1/nil/2", count, err, restored.GetCounter("counter"))
			}
		})
	}
}

func TestPersistentDirtyTrackerObservesDirectAndPartitionedMutations(t *testing.T) {
	for _, partitions := range []int{0, 4} {
		t.Run(fmt.Sprintf("partitions-%d", partitions), func(t *testing.T) {
			trie := newTestTrie(t)
			if err := trie.ConfigureLocalPartitions(partitions); err != nil {
				t.Fatalf("ConfigureLocalPartitions() error = %v", err)
			}
			tracker := NewLevelDBDirtyTracker()
			if err := trie.SetPersistentDirtyTracker(tracker); err != nil {
				t.Fatalf("SetPersistentDirtyTracker() error = %v", err)
			}
			trie.UpsertString("alpha", "one")
			trie.UpsertCounter("counter", 1)
			trie.IncrementCounter("counter", 1)
			trie.Delete("alpha")
			if got := tracker.Snapshot().keys; len(got) != 2 || got[0] != "alpha" || got[1] != "counter" {
				t.Fatalf("tracked keys = %v, want [alpha counter]", got)
			}
		})
	}
}

func TestPersistentAppliedJournalSequencePreventsCounterDoubleReplay(t *testing.T) {
	for _, backend := range []StorageBackend{StorageBackendLevelDB, StorageBackendPebble} {
		t.Run(string(backend), func(t *testing.T) {
			root := t.TempDir()
			storePath := filepath.Join(root, "cache")
			journalPath := filepath.Join(root, "commands.journal")
			store, err := OpenPersistentStoreWithFormat(storePath, backend, StorageFormatBinary)
			if err != nil {
				t.Fatalf("OpenPersistentStoreWithFormat() error = %v", err)
			}
			journal, err := OpenCommandJournal(journalPath)
			if err != nil {
				t.Fatalf("OpenCommandJournal() error = %v", err)
			}
			trie := newTestTrie(t)
			tracker := NewLevelDBDirtyTracker()
			if err := trie.SetPersistentDirtyTracker(tracker); err != nil {
				t.Fatalf("SetPersistentDirtyTracker() error = %v", err)
			}
			trie.UpsertCounter("counter", 0)
			if err := store.SaveWithJournalSequence(trie, 0); err != nil {
				t.Fatalf("SaveWithJournalSequence(0) error = %v", err)
			}
			tracker.Clear(tracker.Snapshot())
			if response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "INC", Key: "counter"}); !response.OK {
				t.Fatalf("INC response = %#v", response)
			}
			if err := journal.WithPersistenceBarrier(func(sequence uint64) error {
				return store.SaveDirtyWithJournalSequence(trie, tracker, LevelDBSaveOptions{}, sequence)
			}); err != nil {
				t.Fatalf("persistent barrier error = %v", err)
			}
			if err := journal.Close(); err != nil {
				t.Fatalf("journal Close() error = %v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatalf("store Close() error = %v", err)
			}

			reopenedStore, err := OpenPersistentStoreWithFormat(storePath, backend, StorageFormatBinary)
			if err != nil {
				t.Fatalf("OpenPersistentStoreWithFormat(reopen) error = %v", err)
			}
			defer reopenedStore.Close()
			restored := newTestTrie(t)
			if _, err := reopenedStore.Load(restored); err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			sequence, ok, err := reopenedStore.AppliedJournalSequence()
			if err != nil || !ok || sequence != 1 {
				t.Fatalf("AppliedJournalSequence() = %d/%v/%v, want 1/true/nil", sequence, ok, err)
			}
			reopenedJournal, err := OpenCommandJournal(journalPath)
			if err != nil {
				t.Fatalf("OpenCommandJournal(reopen) error = %v", err)
			}
			defer reopenedJournal.Close()
			if _, err := reopenedJournal.Replay(restored, sequence); err != nil {
				t.Fatalf("Replay(after applied) error = %v", err)
			}
			if got := restored.GetCounter("counter"); got != 1 {
				t.Fatalf("restored counter = %d, want 1 without double replay", got)
			}
		})
	}
}

func TestPersistentStorageAutoPreservesUnmarkedLevelDB(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy")
	legacy, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	trie := newTestTrie(t)
	trie.UpsertString("legacy", "value")
	if err := legacy.Save(trie); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	store, err := OpenPersistentStoreWithFormat(path, StorageBackendAuto, StorageFormatBinary)
	if err != nil {
		t.Fatalf("OpenPersistentStoreWithFormat(auto) error = %v", err)
	}
	defer store.Close()
	if store.Backend() != StorageBackendLevelDB {
		t.Fatalf("auto backend = %q, want leveldb for unmarked legacy directory", store.Backend())
	}
	restored := newTestTrie(t)
	if _, err := store.Load(restored); err != nil || restored.GetString("legacy") != "value" {
		t.Fatalf("auto legacy load = %q/%v, want value", restored.GetString("legacy"), err)
	}
}

func TestPersistentStorageBackendsShareColdReferenceContract(t *testing.T) {
	for _, backend := range []StorageBackend{StorageBackendLevelDB, StorageBackendPebble} {
		t.Run(string(backend), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "cache")
			store, err := OpenPersistentStoreWithFormat(path, backend, StorageFormatBinary)
			if err != nil {
				t.Fatalf("OpenPersistentStoreWithFormat() error = %v", err)
			}
			defer store.Close()

			source := newTestTrie(t)
			coldValue := strings.Repeat("c", 2048)
			source.UpsertString("cold", coldValue)
			if err := store.Save(source); err != nil {
				t.Fatalf("Save() error = %v", err)
			}

			loaded := newTestTrie(t)
			result, err := store.LoadWithPolicy(loaded, DefaultLevelDBHotLoadPolicy())
			if err != nil {
				t.Fatalf("LoadWithPolicy() error = %v", err)
			}
			if result.KeysLoaded != 1 || result.ValuesLoaded != 0 {
				t.Fatalf("LoadWithPolicy() result = %#v, want one cold key", result)
			}
			entries := loaded.Entries(true)
			if len(entries) != 1 || !entries[0].Value.IsLevelDBReference() {
				t.Fatalf("cold entries = %#v, want one lazy reference", entries)
			}
			if got := loaded.GetString("cold"); got != coldValue {
				t.Fatalf("GetString(cold) = %q, want hydrated value", got)
			}

			loaded.UpsertString("older", strings.Repeat("o", 128))
			spilled, err := store.SpillCold(loaded, LevelDBSpillOptions{
				MaxHotBytes:   128,
				MinValueBytes: 64,
			})
			if err != nil {
				t.Fatalf("SpillCold() error = %v", err)
			}
			if spilled.Store != string(backend) || spilled.KeysSpilled != 1 {
				t.Fatalf("SpillCold() result = %#v, want one %s spill", spilled, backend)
			}
		})
	}
}

func TestPersistentStorageBackendMarkerPreventsWrongEngine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache")
	store, err := OpenPersistentStoreWithFormat(path, StorageBackendPebble, StorageFormatBinary)
	if err != nil {
		t.Fatalf("OpenPersistentStoreWithFormat(pebble) error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if _, err := OpenPersistentStoreWithFormat(path, StorageBackendLevelDB, StorageFormatBinary); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("OpenPersistentStoreWithFormat(leveldb) error = %v, want marker mismatch", err)
	}
	auto, err := OpenPersistentStoreWithFormat(path, StorageBackendAuto, StorageFormatBinary)
	if err != nil {
		t.Fatalf("OpenPersistentStoreWithFormat(auto) error = %v", err)
	}
	defer auto.Close()
	if auto.Backend() != StorageBackendPebble {
		t.Fatalf("auto backend = %q, want marked pebble", auto.Backend())
	}
}

func TestPersistentStorageAutoUsesConfiguredDefaultForNewPath(t *testing.T) {
	if DefaultStorageBackend != StorageBackendPebble {
		t.Fatalf("DefaultStorageBackend = %q, want benchmark-selected pebble", DefaultStorageBackend)
	}
	store, err := OpenPersistentStore(filepath.Join(t.TempDir(), "new-cache"))
	if err != nil {
		t.Fatalf("OpenPersistentStore() error = %v", err)
	}
	defer store.Close()
	if store.Backend() != StorageBackendPebble {
		t.Fatalf("auto backend = %q, want pebble", store.Backend())
	}
}

func TestParseStorageBackendRejectsUnknownValue(t *testing.T) {
	if _, err := ParseStorageBackend("unknown"); err == nil {
		t.Fatal("ParseStorageBackend(unknown) error = nil")
	}
	if got, err := ParseStorageBackend(" LEVELDB "); err != nil || got != StorageBackendLevelDB {
		t.Fatalf("ParseStorageBackend(LEVELDB) = %q/%v", got, err)
	}
	if _, _, err := readStorageBackendMarker(filepath.Join(t.TempDir(), "missing")); err != nil {
		t.Fatalf("readStorageBackendMarker(missing) error = %v", err)
	}
}
