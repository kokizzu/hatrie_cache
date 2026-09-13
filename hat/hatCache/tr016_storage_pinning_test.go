package hatCache

import (
	"strings"
	"testing"
)

func TestStorageKeyPinningDefaultsToOff(t *testing.T) {
	trie := newTestTrie(t)
	if keys := trie.PinnedStorageKeys(); keys != nil {
		t.Fatalf("PinnedStorageKeys() = %#v, want nil when unused", keys)
	}
	if pinned, err := trie.IsStorageKeyPinned("missing"); err != nil || pinned {
		t.Fatalf("IsStorageKeyPinned(missing) = %t/%v, want false/nil", pinned, err)
	}
}

func TestStorageKeyPinningPreservesPinnedValueAcrossSpill(t *testing.T) {
	trie := newTestTrie(t)
	value := strings.Repeat("x", 128)
	trie.UpsertString("pinned", value)
	trie.UpsertString("cold", strings.Repeat("y", 128))

	if err := trie.PinStorageKey("pinned"); err != nil {
		t.Fatalf("PinStorageKey() error = %v", err)
	}
	if pinned, err := trie.IsStorageKeyPinned("pinned"); err != nil || !pinned {
		t.Fatalf("IsStorageKeyPinned(pinned) = %t/%v, want true/nil", pinned, err)
	}

	store, err := OpenLevelDBStore(t.TempDir() + "/store")
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	defer store.Close()
	result, err := store.SpillCold(trie, LevelDBSpillOptions{MaxHotBytes: 0, MinValueBytes: 1})
	if err != nil {
		t.Fatalf("SpillCold() error = %v", err)
	}
	if result.KeysSpilled != 1 {
		t.Fatalf("SpillCold() spilled %d keys, want only cold key: %#v", result.KeysSpilled, result)
	}

	trie.mu.RLock()
	pinnedValue := HatValue{}
	pinnedPtr := trie.tryLocation("pinned")
	if pinnedPtr != nil {
		pinnedValue.fromValue(*pinnedPtr)
	}
	coldValue := HatValue{}
	coldPtr := trie.tryLocation("cold")
	if coldPtr != nil {
		coldValue.fromValue(*coldPtr)
	}
	trie.mu.RUnlock()
	if pinnedPtr == nil || pinnedValue.IsLevelDBReference() {
		t.Fatalf("pinned value = %#v, want materialized value", pinnedValue)
	}
	if coldPtr == nil || !coldValue.IsLevelDBReference() {
		t.Fatalf("cold value = %#v, want LevelDB reference", coldValue)
	}
	if got := trie.GetString("pinned"); got != value {
		t.Fatalf("GetString(pinned) = %q, want original value", got)
	}
}

func TestStorageKeyPinningHydratesExistingColdValue(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("keep", strings.Repeat("k", 128))
	store, err := OpenLevelDBStore(t.TempDir() + "/store")
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	defer store.Close()
	if _, err := store.SpillCold(trie, LevelDBSpillOptions{MaxHotBytes: 0, MinValueBytes: 1}); err != nil {
		t.Fatalf("initial SpillCold() error = %v", err)
	}

	if err := trie.PinStorageKey("keep"); err != nil {
		t.Fatalf("PinStorageKey(cold) error = %v", err)
	}
	trie.mu.RLock()
	value := HatValue{}
	ptr := trie.tryLocation("keep")
	if ptr != nil {
		value.fromValue(*ptr)
	}
	trie.mu.RUnlock()
	if ptr == nil || value.IsLevelDBReference() {
		t.Fatalf("pinned cold value = %#v, want hydrated value", value)
	}
	if result, err := store.SpillCold(trie, LevelDBSpillOptions{MaxHotBytes: 0, MinValueBytes: 1}); err != nil {
		t.Fatalf("SpillCold(pinned cold value) error = %v", err)
	} else if result.KeysSpilled != 0 {
		t.Fatalf("SpillCold(pinned cold value) spilled %d keys, want zero", result.KeysSpilled)
	}
}

func TestStorageKeyPinningUnpinAllowsSpillAndRejectsInvalidKey(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("key", strings.Repeat("v", 128))
	if err := trie.PinStorageKey("key"); err != nil {
		t.Fatalf("PinStorageKey() error = %v", err)
	}
	if err := trie.UnpinStorageKey("key"); err != nil {
		t.Fatalf("UnpinStorageKey() error = %v", err)
	}
	if keys := trie.PinnedStorageKeys(); len(keys) != 0 {
		t.Fatalf("PinnedStorageKeys() after unpin = %#v, want empty", keys)
	}
	if err := trie.PinStorageKey(strings.Repeat("x", maxHATTrieKeyLength+1)); err == nil {
		t.Fatal("PinStorageKey(oversized) error = nil")
	}

	store, err := OpenLevelDBStore(t.TempDir() + "/store")
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	defer store.Close()
	result, err := store.SpillCold(trie, LevelDBSpillOptions{MaxHotBytes: 0, MinValueBytes: 1})
	if err != nil {
		t.Fatalf("SpillCold() error = %v", err)
	}
	if result.KeysSpilled != 1 {
		t.Fatalf("SpillCold() spilled %d keys after unpin, want 1", result.KeysSpilled)
	}
}

func TestStorageKeyPinningPreservesPinsWhenPartitionsAreConfigured(t *testing.T) {
	trie, err := CreateHatTrieWithDiskDir(t.TempDir()+"/root", true)
	if err != nil {
		t.Fatalf("CreateHatTrieWithDiskDir() error = %v", err)
	}
	defer trie.Destroy()

	if err := trie.PinStorageKey("before"); err != nil {
		t.Fatalf("PinStorageKey(before) error = %v", err)
	}
	if err := trie.ConfigureLocalPartitions(2); err != nil {
		t.Fatalf("ConfigureLocalPartitions() error = %v", err)
	}
	trie.UpsertString("before", "value-before")
	trie.UpsertString("after", "value-after")
	if err := trie.PinStorageKey("after"); err != nil {
		t.Fatalf("PinStorageKey(after) error = %v", err)
	}

	for _, key := range []string{"before", "after"} {
		pinned, err := trie.IsStorageKeyPinned(key)
		if err != nil || !pinned {
			t.Fatalf("IsStorageKeyPinned(%q) = %t/%v, want true/nil", key, pinned, err)
		}
	}
	keys := trie.PinnedStorageKeys()
	if len(keys) != 2 || keys[0] != "after" || keys[1] != "before" {
		t.Fatalf("PinnedStorageKeys() = %#v, want sorted pins", keys)
	}
	keys[0] = "changed"
	if fresh := trie.PinnedStorageKeys(); len(fresh) != 2 || fresh[0] != "after" {
		t.Fatalf("PinnedStorageKeys() returned aliased data: %#v", fresh)
	}
}

func TestStorageKeyPinningPreservesPinnedValueAcrossPebbleSpill(t *testing.T) {
	trie := newTestTrie(t)
	value := strings.Repeat("p", 128)
	trie.UpsertString("pinned", value)
	trie.UpsertString("cold", strings.Repeat("c", 128))
	if err := trie.PinStorageKey("pinned"); err != nil {
		t.Fatalf("PinStorageKey() error = %v", err)
	}

	store, err := OpenPersistentStoreWithFormat(t.TempDir()+"/store", StorageBackendPebble, StorageFormatBinary)
	if err != nil {
		t.Fatalf("OpenPersistentStoreWithFormat() error = %v", err)
	}
	defer store.Close()
	result, err := store.SpillCold(trie, LevelDBSpillOptions{MaxHotBytes: 0, MinValueBytes: 1})
	if err != nil {
		t.Fatalf("SpillCold() error = %v", err)
	}
	if result.Store != string(StorageBackendPebble) || result.KeysSpilled != 1 {
		t.Fatalf("SpillCold() result = %#v, want one Pebble spill", result)
	}

	trie.mu.RLock()
	pinnedValue := HatValue{}
	pinnedPtr := trie.tryLocation("pinned")
	if pinnedPtr != nil {
		pinnedValue.fromValue(*pinnedPtr)
	}
	trie.mu.RUnlock()
	if pinnedPtr == nil || pinnedValue.IsLevelDBReference() {
		t.Fatalf("pinned value = %#v, want materialized value", pinnedValue)
	}
	if got := trie.GetString("pinned"); got != value {
		t.Fatalf("GetString(pinned) = %q, want original value", got)
	}
}
