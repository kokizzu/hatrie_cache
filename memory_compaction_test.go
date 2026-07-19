package hatriecache

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"
)

func TestCompactMemoryPreservesTypedValuesAndReclaimsBacking(t *testing.T) {
	trie, _ := benchmarkStructuredSnapshotTrie(t)
	t.Cleanup(trie.Destroy)
	now := time.Unix(20_000, 0)
	trie.now = func() time.Time { return now }

	for group := 0; group < benchmarkStructuredSnapshotGroups; group++ {
		key := fmt.Sprintf("disk:%02d", group)
		trie.UpsertBytes(key, bytes.Repeat([]byte{byte(group + 1)}, DiskBytesThreshold+1))
	}
	trie.UpsertString("", "empty-key-value")
	if !trie.Expire("structured:12:string", time.Hour) {
		t.Fatal("Expire() = false, want true")
	}
	if _, err := trie.replicationMerkleSnapshot(); err != nil {
		t.Fatalf("replicationMerkleSnapshot() error = %v", err)
	}

	for group := 0; group < benchmarkStructuredSnapshotGroups; group++ {
		if group%4 == 0 {
			continue
		}
		prefix := fmt.Sprintf("structured:%02d", group)
		for _, key := range trie.KeysWithPrefix(prefix, false) {
			trie.Delete(key)
		}
		trie.Delete(fmt.Sprintf("disk:%02d", group))
	}

	wantEntries := trie.Entries(true)
	wantDumps := make(map[string][]byte, len(wantEntries))
	for _, entry := range wantEntries {
		data, ok, err := trie.commandDumpEntryBinaryWithoutStats(entry.Key)
		if err != nil || !ok {
			t.Fatalf("commandDumpEntryBinaryWithoutStats(%q) = %v/%v", entry.Key, ok, err)
		}
		wantDumps[entry.Key] = data
	}
	wantMerkle, err := trie.replicationMerkleSnapshot()
	if err != nil {
		t.Fatalf("replicationMerkleSnapshot(before) error = %v", err)
	}
	wantTTL := trie.TTL("structured:12:string")
	wantStats := trie.Stats()

	result, err := trie.CompactMemory()
	if err != nil {
		t.Fatalf("CompactMemory() error = %v", err)
	}
	if result.Entries != len(wantEntries) {
		t.Fatalf("CompactMemory().Entries = %d, want %d", result.Entries, len(wantEntries))
	}
	if result.BackingBytesAfter >= result.BackingBytesBefore {
		t.Fatalf("CompactMemory() backing bytes = %d -> %d, want reduction", result.BackingBytesBefore, result.BackingBytesAfter)
	}
	if result.MerkleBytesAfter >= result.MerkleBytesBefore {
		t.Fatalf("CompactMemory() Merkle bytes = %d -> %d, want reduction", result.MerkleBytesBefore, result.MerkleBytesAfter)
	}
	if got := trie.Stats(); got != wantStats {
		t.Fatalf("Stats() after compaction = %#v, want %#v", got, wantStats)
	}
	if got := trie.TTL("structured:12:string"); got != wantTTL {
		t.Fatalf("TTL() after compaction = %v, want %v", got, wantTTL)
	}
	gotMerkle, err := trie.replicationMerkleSnapshot()
	if err != nil {
		t.Fatalf("replicationMerkleSnapshot(after) error = %v", err)
	}
	if !gotMerkle.equal(wantMerkle) {
		t.Fatalf("Merkle snapshot changed: %#v -> %#v", wantMerkle, gotMerkle)
	}
	for key, want := range wantDumps {
		got, ok, err := trie.commandDumpEntryBinaryWithoutStats(key)
		if err != nil || !ok {
			t.Fatalf("commandDumpEntryBinaryWithoutStats(%q) after = %v/%v", key, ok, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("entry %q changed across compaction", key)
		}
	}
	trie.Delete("disk:12")
	trie.UpsertBytes("disk:new", bytes.Repeat([]byte{0xff}, DiskBytesThreshold+1))
	if got := trie.GetBytes("disk:00"); len(got) != DiskBytesThreshold+1 || got[0] != 1 {
		t.Fatalf("disk:00 changed after compacted disk-index reuse: len=%d first=%d", len(got), got[0])
	}
	assertCompactedStorageIndexes(t, trie)
}

func TestCompactMemoryPreservesKeyStatsAndHotAccounting(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.ConfigureKeyStats(KeyStatsModeBounded, 8); err != nil {
		t.Fatal(err)
	}
	for idx := 0; idx < 32; idx++ {
		trie.UpsertString(fmt.Sprintf("key:%02d", idx), "value")
	}
	for idx := 0; idx < 24; idx++ {
		trie.Delete(fmt.Sprintf("key:%02d", idx))
	}
	trie.GetString("key:24")
	want, ok := trie.StatsForKey("key:24")
	if !ok {
		t.Fatal("StatsForKey(key:24) missing before compaction")
	}
	trie.mu.Lock()
	trie.levelDBSpillKeys = map[string]struct{}{"key:24": {}}
	trie.setLevelDBHotByteAccountingLocked(map[string]int64{"key:24": 5}, 5)
	trie.mu.Unlock()

	if _, err := trie.CompactMemory(); err != nil {
		t.Fatalf("CompactMemory() error = %v", err)
	}
	got, ok := trie.StatsForKey("key:24")
	if !ok || got != want {
		t.Fatalf("StatsForKey(key:24) = %#v/%v, want %#v/true", got, ok, want)
	}
	if trie.GetString("key:31") != "value" {
		t.Fatal("live value missing after compaction")
	}
	trie.mu.RLock()
	_, spillTracked := trie.levelDBSpillKeys["key:24"]
	hotBytes := trie.levelDBHotValues["key:24"]
	hotTotal := trie.levelDBHotBytes
	trie.mu.RUnlock()
	if !spillTracked || hotBytes != 5 || hotTotal != 5 {
		t.Fatalf("LevelDB accounting after compaction = %v/%d/%d, want true/5/5", spillTracked, hotBytes, hotTotal)
	}
}

func TestCompactMemoryKeepsDiskPathIndexesCollisionFree(t *testing.T) {
	trie := newTestTrie(t)
	payloadA := bytes.Repeat([]byte{'a'}, DiskBytesThreshold+1)
	payloadB := bytes.Repeat([]byte{'b'}, DiskBytesThreshold+1)
	payloadC := bytes.Repeat([]byte{'c'}, DiskBytesThreshold+1)
	trie.UpsertBytes("a", payloadA)
	trie.UpsertBytes("b", payloadB)
	trie.UpsertBytes("c", payloadC)
	trie.Delete("a")

	if _, err := trie.CompactMemory(); err != nil {
		t.Fatalf("CompactMemory() error = %v", err)
	}
	trie.Delete("c")
	trie.UpsertBytes("d", bytes.Repeat([]byte{'d'}, DiskBytesThreshold+1))
	if got := trie.GetBytes("b"); !bytes.Equal(got, payloadB) {
		t.Fatal("reused compacted disk index overwrote a live value")
	}
}

func TestCompactMemoryRestartsPagedCursorWithoutDuplicates(t *testing.T) {
	trie := newTestTrie(t)
	for idx := 0; idx < 32; idx++ {
		trie.UpsertString(fmt.Sprintf("key:%02d", idx), "value")
	}
	cursor := &replicationSyncCursor{}
	defer cursor.close(trie)
	seen := make(map[string]struct{})
	page, err := replicationSyncEntriesPageWithCursor(trie, "", "", false, 7, cursor, func(entry Entry) error {
		seen[entry.Key] = struct{}{}
		return nil
	})
	if err != nil || !page.hasMore {
		t.Fatalf("first page = %#v/%v, want continuation", page, err)
	}
	if _, err := trie.CompactMemory(); err != nil {
		t.Fatalf("CompactMemory() error = %v", err)
	}
	afterKey := page.nextAfterKey
	for page.hasMore {
		page, err = replicationSyncEntriesPageWithCursor(trie, "", afterKey, true, 7, cursor, func(entry Entry) error {
			if _, duplicate := seen[entry.Key]; duplicate {
				t.Fatalf("duplicate key %q after cursor restart", entry.Key)
			}
			seen[entry.Key] = struct{}{}
			return nil
		})
		if err != nil {
			t.Fatalf("continuation page error = %v", err)
		}
		afterKey = page.nextAfterKey
	}
	if len(seen) != 32 {
		t.Fatalf("paged scan returned %d keys, want 32", len(seen))
	}
	if cursor.restarts != 1 {
		t.Fatalf("cursor restarts = %d, want 1", cursor.restarts)
	}
}

func TestCompactMemoryRejectsNilTrie(t *testing.T) {
	var trie *HatTrie
	if _, err := trie.CompactMemory(); err != ErrNilHatTrie {
		t.Fatalf("CompactMemory(nil) error = %v, want ErrNilHatTrie", err)
	}
}

func TestCompactMemoryValidationFailureLeavesLiveTrieUntouched(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("key", "value")
	trie.mu.Lock()
	root := trie.root
	trie.raws.reusables.Mark(0)
	trie.mu.Unlock()

	if _, err := trie.CompactMemory(); err == nil {
		t.Fatal("CompactMemory() error = nil, want invalid backing-index error")
	}
	trie.mu.Lock()
	if trie.root != root {
		trie.mu.Unlock()
		t.Fatal("CompactMemory() replaced root after validation failure")
	}
	trie.raws.reusables.Use(0)
	trie.mu.Unlock()
	if got := trie.GetString("key"); got != "value" {
		t.Fatalf("GetString(key) after failed compaction = %q, want value", got)
	}
}

func TestMemoryCompactorCompactsChangedTrie(t *testing.T) {
	trie := newTestTrie(t)
	for idx := 0; idx < 2048; idx++ {
		trie.UpsertString(fmt.Sprintf("key:%04d", idx), "value")
	}
	for idx := 0; idx < 2048; idx++ {
		if idx%16 != 0 {
			trie.Delete(fmt.Sprintf("key:%04d", idx))
		}
	}
	trie.mu.RLock()
	before := trie.memoryBackingBytesLocked()
	trie.mu.RUnlock()
	stop := trie.StartMemoryCompactor(time.Millisecond)
	defer stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		trie.mu.RLock()
		after := trie.memoryBackingBytesLocked()
		trie.mu.RUnlock()
		if after < before {
			if trie.Size() != 128 {
				t.Fatalf("compacted trie size = %d, want 128", trie.Size())
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("background memory compactor did not reclaim backing storage")
}

func TestMemoryCompactorStopsWithContextAndDestroyedTrie(t *testing.T) {
	trie := newTestTrie(t)
	ctx, cancel := context.WithCancel(context.Background())
	stop := trie.StartMemoryCompactorContext(ctx, time.Millisecond)
	cancel()
	stop()

	trie.Destroy()
	stop = trie.StartMemoryCompactor(time.Millisecond)
	stop()
}

func TestMemoryCompactorRejectsInvalidInterval(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("StartMemoryCompactor(0) did not panic")
		}
	}()
	newTestTrie(t).StartMemoryCompactor(0)
}

func assertCompactedStorageIndexes(t *testing.T, trie *HatTrie) {
	t.Helper()
	trie.mu.RLock()
	defer trie.mu.RUnlock()
	for _, indexes := range []*reusableIndexes{
		&trie.raws.reusables, &trie.maps.reusables,
		&trie.slices.reusables, &trie.sets.reusables, &trie.priorityQueues.reusables,
		&trie.bloomFilters.reusables, &trie.countMinSketches.reusables,
		&trie.hyperLogLogs.reusables, &trie.topKs.reusables, &trie.cuckooFilters.reusables,
		&trie.roaringBitmaps.reusables, &trie.quantileSketches.reusables,
		&trie.fenwickTrees.reusables, &trie.sparseBitsets.reusables,
		&trie.reservoirSamples.reusables, &trie.xorFilters.reusables,
		&trie.radixTrees.reusables, &trie.dbrefs.reusables,
	} {
		if indexes.Len() != 0 || len(indexes.stack) != 0 || len(indexes.bits) != 0 {
			t.Fatalf("compacted storage retained reusable indexes: %#v", indexes)
		}
	}
}
