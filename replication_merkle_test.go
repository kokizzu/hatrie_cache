package hatriecache

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestReplicationMerkleIndexTracksMutationsWithinCompactBudget(t *testing.T) {
	const keys = 10_000
	left := newTestTrie(t)
	right := newTestTrie(t)
	for idx := 0; idx < keys; idx++ {
		key := fmt.Sprintf("session:%05d", idx)
		value := fmt.Sprintf("value:%05d", idx)
		left.UpsertString(key, value)
		right.UpsertString(key, value)
	}

	leftSnapshot, err := left.replicationMerkleSnapshot()
	if err != nil {
		t.Fatalf("left replicationMerkleSnapshot() error = %v", err)
	}
	rightSnapshot, err := right.replicationMerkleSnapshot()
	if err != nil {
		t.Fatalf("right replicationMerkleSnapshot() error = %v", err)
	}
	if !leftSnapshot.equal(rightSnapshot) || leftSnapshot.count != keys {
		t.Fatalf("initial Merkle snapshots differ: left=%#v right=%#v", leftSnapshot, rightSnapshot)
	}
	if got := left.replicationMerkleRetainedBytes(); got > keys*32 {
		t.Fatalf("Merkle retained bytes = %d, want <= %d (32 B/key)", got, keys*32)
	}

	left.UpsertString("session:00001", "changed")
	left.Delete("session:00002")
	left.UpsertMap("session:00003", Map{"state": "structured"})
	changed, err := left.replicationMerkleSnapshot()
	if err != nil {
		t.Fatalf("changed replicationMerkleSnapshot() error = %v", err)
	}
	mask := changed.changedBuckets(rightSnapshot)
	if mask.empty() {
		t.Fatal("changed Merkle snapshots produced an empty bucket mask")
	}

	right.UpsertString("session:00001", "changed")
	right.Delete("session:00002")
	right.UpsertMap("session:00003", Map{"state": "structured"})
	repaired, err := right.replicationMerkleSnapshot()
	if err != nil {
		t.Fatalf("repaired replicationMerkleSnapshot() error = %v", err)
	}
	if !changed.equal(repaired) {
		t.Fatalf("Merkle snapshots differ after equivalent mutations: left=%#v right=%#v", changed, repaired)
	}
}

func TestReplicationMerkleDefersAndCoalescesMutationsUntilSnapshot(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("session:1", "one")
	trie.UpsertString("session:2", "two")
	initial, err := trie.replicationMerkleSnapshot()
	if err != nil {
		t.Fatalf("initial replicationMerkleSnapshot() error = %v", err)
	}

	trie.UpsertString("session:1", "changed-once")
	trie.UpsertString("session:1", "changed-final")
	trie.Delete("session:2")

	trie.mu.RLock()
	pending := trie.replicationMerkle.pendingCount()
	stale := trie.replicationMerkle.snapshot()
	trie.mu.RUnlock()
	if pending != 2 {
		t.Fatalf("pending Merkle keys = %d, want 2 unique keys", pending)
	}
	if !stale.equal(initial) {
		t.Fatal("Merkle leaves changed before the snapshot flush")
	}

	got, err := trie.replicationMerkleSnapshot()
	if err != nil {
		t.Fatalf("flushed replicationMerkleSnapshot() error = %v", err)
	}
	if got.equal(initial) || got.count != 1 {
		t.Fatalf("flushed Merkle root/count = %x/%d, want one changed entry", got.root, got.count)
	}
	trie.mu.RLock()
	pending = trie.replicationMerkle.pendingCount()
	trie.mu.RUnlock()
	if pending != 0 {
		t.Fatalf("pending Merkle keys after snapshot = %d, want 0", pending)
	}

	wantTrie := newTestTrie(t)
	wantTrie.UpsertString("session:1", "changed-final")
	want, err := wantTrie.replicationMerkleSnapshot()
	if err != nil {
		t.Fatalf("expected replicationMerkleSnapshot() error = %v", err)
	}
	if !got.equal(want) {
		t.Fatalf("deferred/rebuilt Merkle root/count = %x/%d and %x/%d, want equal", got.root, got.count, want.root, want.count)
	}
}

func TestReplicationMerklePendingOverflowFallsBackToRebuild(t *testing.T) {
	trie := newTestTrie(t)
	wantTrie := newTestTrie(t)
	for idx := 0; idx <= replicationMerkleMaxPendingKeys; idx++ {
		key := fmt.Sprintf("session:%05d", idx)
		trie.UpsertString(key, "before")
		wantTrie.UpsertString(key, "after")
	}
	if _, err := trie.replicationMerkleSnapshot(); err != nil {
		t.Fatalf("initial replicationMerkleSnapshot() error = %v", err)
	}
	for idx := 0; idx <= replicationMerkleMaxPendingKeys; idx++ {
		trie.UpsertString(fmt.Sprintf("session:%05d", idx), "after")
	}
	trie.mu.RLock()
	valid := trie.replicationMerkle.valid
	pending := trie.replicationMerkle.pendingCount()
	trie.mu.RUnlock()
	if valid {
		t.Fatal("Merkle index remained valid after pending-key limit")
	}
	if pending != 0 {
		t.Fatalf("pending Merkle keys after overflow = %d, want 0", pending)
	}

	got, err := trie.replicationMerkleSnapshot()
	if err != nil {
		t.Fatalf("rebuilt replicationMerkleSnapshot() error = %v", err)
	}
	want, err := wantTrie.replicationMerkleSnapshot()
	if err != nil {
		t.Fatalf("expected replicationMerkleSnapshot() error = %v", err)
	}
	if !got.equal(want) {
		t.Fatalf("overflow/rebuilt Merkle root/count = %x/%d and %x/%d, want equal", got.root, got.count, want.root, want.count)
	}
}

func TestReplicationMerkleTracksEmptyKeyMutation(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("", "before")
	before, err := trie.replicationMerkleSnapshot()
	if err != nil {
		t.Fatalf("initial replicationMerkleSnapshot() error = %v", err)
	}

	trie.UpsertString("", "after")
	after, err := trie.replicationMerkleSnapshot()
	if err != nil {
		t.Fatalf("updated replicationMerkleSnapshot() error = %v", err)
	}
	if after.equal(before) || after.count != 1 {
		t.Fatalf("empty-key Merkle root/count = %x/%d -> %x/%d, want changed one-entry root", before.root, before.count, after.root, after.count)
	}
}

func TestReplicationMerkleTableDeletePreservesCollisionChain(t *testing.T) {
	table := newReplicationMerkleTable()
	mask := uint64(len(table.keys) - 1)
	keys := []uint64{7, 8, 7 + mask + 1}
	for index, key := range keys {
		table.set(key, uint64(index+1))
	}

	if previous, ok := table.delete(keys[0]); !ok || previous != 1 {
		t.Fatalf("delete(%d) = (%d, %t), want (1, true)", keys[0], previous, ok)
	}
	for index, key := range keys[1:] {
		if value, ok := table.get(key); !ok || value != uint64(index+2) {
			t.Fatalf("get(%d) after collision delete = (%d, %t), want (%d, true)", key, value, ok, index+2)
		}
	}
	if table.count != len(keys)-1 {
		t.Fatalf("table count = %d, want %d", table.count, len(keys)-1)
	}
}

func TestReplicationMerkleTableUpdateDoesNotResize(t *testing.T) {
	table := newReplicationMerkleTable()
	for key := uint64(0); (table.count+1)*10 <= len(table.keys)*7; key++ {
		table.set(key, key+1)
	}
	capacity := len(table.keys)
	count := table.count
	previous, existed := table.set(0, 99)
	if !existed || previous != 1 {
		t.Fatalf("set(existing) = (%d, %t), want (1, true)", previous, existed)
	}
	if len(table.keys) != capacity || table.count != count {
		t.Fatalf("existing update changed table from capacity/count %d/%d to %d/%d", capacity, count, len(table.keys), table.count)
	}
}

func TestReplicationMerkleDestroyReleasesIndex(t *testing.T) {
	trie := CreateHatTrie()
	trie.UpsertString("session:1", "value")
	if _, err := trie.replicationMerkleSnapshot(); err != nil {
		t.Fatalf("replicationMerkleSnapshot() error = %v", err)
	}
	if got := trie.replicationMerkleRetainedBytes(); got == 0 {
		t.Fatal("active Merkle index retained bytes = 0")
	}

	trie.Destroy()
	if got := trie.replicationMerkleRetainedBytes(); got != 0 {
		t.Fatalf("retained bytes after Destroy() = %d, want 0", got)
	}
}

func TestReplicationMerkleSnapshotLoadInvalidatesActiveIndex(t *testing.T) {
	source := newTestTrie(t)
	source.UpsertString("session:1", "restored")
	source.UpsertString("session:2", "added")
	path := t.TempDir() + "/snapshot.json.gz"
	if err := source.SaveSnapshot(path); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}

	target := newTestTrie(t)
	target.UpsertString("session:1", "old")
	target.UpsertString("session:stale", "remove")
	if _, err := target.replicationMerkleSnapshot(); err != nil {
		t.Fatalf("initial target replicationMerkleSnapshot() error = %v", err)
	}
	if err := target.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}

	sourceRoot, err := source.replicationMerkleSnapshot()
	if err != nil {
		t.Fatalf("source replicationMerkleSnapshot() error = %v", err)
	}
	targetRoot, err := target.replicationMerkleSnapshot()
	if err != nil {
		t.Fatalf("target replicationMerkleSnapshot() error = %v", err)
	}
	if !sourceRoot.equal(targetRoot) {
		t.Fatal("Merkle roots differ after loading a snapshot into an active index")
	}
}

func TestReplicationMerkleEqualAndSparseRepair(t *testing.T) {
	const keys = 10_000
	source := newTestTrie(t)
	target := newTestTrie(t)
	for idx := 0; idx < keys; idx++ {
		key := fmt.Sprintf("session:%05d", idx)
		value := fmt.Sprintf("value:%05d", idx)
		source.UpsertString(key, value)
		target.UpsertString(key, value)
	}
	if _, err := source.replicationMerkleSnapshot(); err != nil {
		t.Fatalf("source replicationMerkleSnapshot() error = %v", err)
	}
	if _, err := target.replicationMerkleSnapshot(); err != nil {
		t.Fatalf("target replicationMerkleSnapshot() error = %v", err)
	}

	var requests atomic.Int64
	var handler http.Handler
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	topology := replicationTestTopology(t, server.URL)
	handler = NewMonitoringHandler(target, MonitoringOptions{
		NodeName:          "node-b",
		Topology:          topology,
		Election:          NewElectionStore(topology, ElectionOptions{}),
		ReplicationSafety: NewReplicationSafetyStore(),
	}).Handler()
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: NewElectionStore(topology, ElectionOptions{}),
		Client:   server.Client(),
	})
	t.Cleanup(replicator.Close)

	equal := replicator.syncAllPaged(context.Background(), source, "", defaultReplicationSyncKeyPageSize)
	if equal.Skipped || !strings.Contains(equal.Reason, "merkle equal") {
		t.Fatalf("equal sync = %#v, want Merkle equality fast path", equal)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("equal Merkle requests = %d, want one root request", got)
	}

	for idx := 0; idx < 100; idx++ {
		source.UpsertString(fmt.Sprintf("session:%05d", idx), fmt.Sprintf("changed:%05d", idx))
	}
	source.Delete("session:09999")
	requests.Store(0)
	repaired := replicator.syncAllPaged(context.Background(), source, "", defaultReplicationSyncKeyPageSize)
	if repaired.Skipped || !strings.Contains(repaired.Reason, "merkle") || !strings.Contains(repaired.Reason, "transferred 100, deleted 1") {
		t.Fatalf("sparse Merkle sync = %#v, want 100 changes and one deletion", repaired)
	}
	for idx := 0; idx < 100; idx++ {
		key := fmt.Sprintf("session:%05d", idx)
		if got, want := target.GetString(key), fmt.Sprintf("changed:%05d", idx); got != want {
			t.Fatalf("target %s = %q, want %q", key, got, want)
		}
	}
	if target.Exists("session:09999") {
		t.Fatal("sparse Merkle repair retained deleted target key")
	}
	sourceRoot, err := source.replicationMerkleSnapshot()
	if err != nil {
		t.Fatalf("source root after repair error = %v", err)
	}
	targetRoot, err := target.replicationMerkleSnapshot()
	if err != nil {
		t.Fatalf("target root after repair error = %v", err)
	}
	if !sourceRoot.equal(targetRoot) {
		t.Fatal("source and target Merkle roots differ after sparse repair")
	}
}
