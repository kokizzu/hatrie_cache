package hatSql_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"hatrie_cache/hat/hatSql"
)

type m226MetadataStore struct {
	mu       sync.Mutex
	snapshot hatSql.SQLShardConsensusMetadataSnapshot
	found    bool
	commits  int
}

func (store *m226MetadataStore) Load(context.Context) (hatSql.SQLShardConsensusMetadataSnapshot, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.snapshot, store.found, nil
}

func (store *m226MetadataStore) Commit(_ context.Context, expectedGeneration uint64, snapshot hatSql.SQLShardConsensusMetadataSnapshot) (bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	currentGeneration := uint64(0)
	if store.found {
		currentGeneration = store.snapshot.Generation
	}
	if currentGeneration != expectedGeneration {
		return false, nil
	}
	store.snapshot = snapshot
	store.found = true
	store.commits++
	return true, nil
}

func m226Registry(t *testing.T) *hatSql.SQLShardConsensusMetadataRegistry {
	t.Helper()
	registry, err := hatSql.NewSQLShardConsensusMetadataRegistry(hatSql.SQLShardConsensusMetadataRegistryOptions{
		ShardCount:      4,
		MaxShards:       16,
		MaxShardIDBytes: 64,
		MaxOwnerBytes:   64,
	})
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func TestM226ConsensusMetadataFencesOwnersAndAdvancesFrontier(t *testing.T) {
	registry := m226Registry(t)
	first, err := registry.Claim("region-a/0", "worker-a", 7)
	if err != nil {
		t.Fatalf("Claim(first) error = %v", err)
	}
	if first.Term != 1 || first.Revision != 1 || first.Generation != 1 || first.FrontierObserved {
		t.Fatalf("first metadata = %#v", first)
	}
	advanced, err := registry.AdvanceFrontier("region-a/0", "worker-a", 7, 10)
	if err != nil {
		t.Fatalf("AdvanceFrontier() error = %v", err)
	}
	if advanced.Frontier != 10 || !advanced.FrontierObserved || advanced.Revision != 2 || advanced.Generation != 2 {
		t.Fatalf("advanced metadata = %#v", advanced)
	}
	if _, err := registry.AdvanceFrontier("region-a/0", "worker-a", 7, 9); !errors.Is(err, hatSql.ErrSQLShardConsensusFrontierRegression) {
		t.Fatalf("frontier regression error = %v", err)
	}
	if _, err := registry.Claim("region-a/0", "worker-b", 7); !errors.Is(err, hatSql.ErrSQLShardConsensusStaleOwner) {
		t.Fatalf("same-token takeover error = %v", err)
	}
	second, err := registry.Claim("region-a/0", "worker-b", 8)
	if err != nil {
		t.Fatalf("Claim(takeover) error = %v", err)
	}
	if second.Term != 2 || second.FencingToken != 8 || second.Frontier != 10 || second.Generation != 3 {
		t.Fatalf("takeover metadata = %#v", second)
	}
	if _, err := registry.AdvanceFrontier("region-a/0", "worker-a", 7, 11); !errors.Is(err, hatSql.ErrSQLShardConsensusStaleOwner) {
		t.Fatalf("stale frontier update error = %v", err)
	}
}

func TestM226ConsensusMetadataCheckpointCASAndRestore(t *testing.T) {
	registry := m226Registry(t)
	if _, err := registry.Claim("region-b/0", "worker-b", 3); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Claim("region-a/0", "worker-a", 2); err != nil {
		t.Fatal(err)
	}
	first, err := registry.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	second, err := registry.MarshalBinary()
	if err != nil {
		t.Fatalf("second MarshalBinary() error = %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("MarshalBinary() is not deterministic")
	}
	tampered := append([]byte(nil), first...)
	tampered[len(tampered)-5] ^= 1
	if _, err := hatSql.UnmarshalSQLShardConsensusMetadataSnapshot(tampered); !errors.Is(err, hatSql.ErrSQLShardConsensusSnapshotInvalid) {
		t.Fatalf("tampered snapshot error = %v", err)
	}
	snapshot, err := hatSql.UnmarshalSQLShardConsensusMetadataSnapshot(first)
	if err != nil {
		t.Fatalf("UnmarshalSQLShardConsensusMetadataSnapshot() error = %v", err)
	}
	if snapshot.Shards[0].ShardID != "region-a/0" || snapshot.Shards[1].ShardID != "region-b/0" {
		t.Fatalf("snapshot order = %#v", snapshot.Shards)
	}

	store := &m226MetadataStore{}
	committed, err := registry.CommitTo(context.Background(), store, 0)
	if err != nil || !committed {
		t.Fatalf("CommitTo() = %v, %v", committed, err)
	}
	conflict, err := registry.CommitTo(context.Background(), store, 0)
	if err != nil || conflict {
		t.Fatalf("conflicting CommitTo() = %v, %v", conflict, err)
	}
	restored := m226Registry(t)
	found, err := restored.RestoreFrom(context.Background(), store)
	if err != nil || !found {
		t.Fatalf("RestoreFrom() = %v, %v", found, err)
	}
	got, ok := restored.Get("region-a/0")
	if !ok || got.Owner != "worker-a" || got.FencingToken != 2 || got.Generation != 2 {
		t.Fatalf("restored metadata = %#v, found=%v", got, ok)
	}
}

func TestM226ConsensusMetadataConcurrentFrontierUpdatesRemainMonotone(t *testing.T) {
	registry := m226Registry(t)
	if _, err := registry.Claim("region-c/0", "worker-c", 11); err != nil {
		t.Fatal(err)
	}
	const callers = 32
	var group sync.WaitGroup
	group.Add(callers)
	for index := 1; index <= callers; index++ {
		go func(frontier uint64) {
			defer group.Done()
			_, _ = registry.AdvanceFrontier("region-c/0", "worker-c", 11, frontier)
		}(uint64(index))
	}
	group.Wait()
	metadata, ok := registry.Get("region-c/0")
	if !ok || metadata.Frontier != callers || !metadata.FrontierObserved {
		t.Fatalf("concurrent frontier metadata = %#v, found=%v", metadata, ok)
	}
}

func TestM226ConsensusMetadataNormalizesIdentity(t *testing.T) {
	registry := m226Registry(t)
	metadata, err := registry.Claim(" region-d/0 ", " worker-d ", 1)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.ShardID != "region-d/0" || metadata.Owner != "worker-d" {
		t.Fatalf("normalized metadata = %#v", metadata)
	}
	if _, ok := registry.Get("region-d/0"); !ok {
		t.Fatal("Get() did not find normalized shard")
	}
	restored := m226Registry(t)
	if err := restored.Restore(hatSql.SQLShardConsensusMetadataSnapshot{
		Generation: 1,
		Shards: []hatSql.SQLShardConsensusMetadata{{
			ShardID:      " region-e/0 ",
			Owner:        " worker-e ",
			Term:         1,
			FencingToken: 2,
			Revision:     1,
			Generation:   1,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	metadata, ok := restored.Get("region-e/0")
	if !ok || metadata.ShardID != "region-e/0" || metadata.Owner != "worker-e" {
		t.Fatalf("restored normalized metadata = %#v, found=%v", metadata, ok)
	}
}
