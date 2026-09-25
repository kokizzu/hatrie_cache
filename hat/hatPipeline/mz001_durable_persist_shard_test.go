package hatPipeline

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

type mz001MemoryStore struct {
	mu      sync.Mutex
	payload []byte
}

func (store *mz001MemoryStore) Save(ctx context.Context, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mu.Lock()
	store.payload = append(store.payload[:0], payload...)
	store.mu.Unlock()
	return nil
}

func (store *mz001MemoryStore) Load(ctx context.Context) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return append([]byte(nil), store.payload...), nil
}

func TestMZ001DurablePersistShardRoundTripWithoutSourceRead(t *testing.T) {
	shard, err := NewDurablePersistShard(DurablePersistShardOptions{ShardID: "orders-eu", MaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("NewDurablePersistShard() error = %v", err)
	}
	payload := []byte("materialized-order-state")
	if err := shard.Publish(7, 42, payload); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	payload[0] = 'X'
	store := new(mz001MemoryStore)
	if err := shard.Save(context.Background(), store); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	restored, err := NewDurablePersistShard(DurablePersistShardOptions{ShardID: "orders-eu", MaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("NewDurablePersistShard(restored) error = %v", err)
	}
	found, err := restored.Hydrate(context.Background(), store)
	if err != nil || !found {
		t.Fatalf("Hydrate() = found %v, error %v; want found", found, err)
	}
	snapshot, ok := restored.Snapshot()
	if !ok {
		t.Fatal("Snapshot() reports no hydrated state")
	}
	if snapshot.ShardID != "orders-eu" || snapshot.Generation != 7 || snapshot.Upper != 42 || string(snapshot.Payload) != "materialized-order-state" {
		t.Fatalf("snapshot = %#v, want restored shard state", snapshot)
	}
	snapshot.Payload[0] = 'Y'
	again, _ := restored.Snapshot()
	if string(again.Payload) != "materialized-order-state" {
		t.Fatal("Snapshot() exposed mutable payload")
	}
}

func TestMZ001DurablePersistShardRejectsStaleAndCorruptStateAtomically(t *testing.T) {
	shard, err := NewDurablePersistShard(DurablePersistShardOptions{ShardID: "orders-us"})
	if err != nil {
		t.Fatalf("NewDurablePersistShard() error = %v", err)
	}
	if err := shard.Publish(2, 20, []byte("new")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	before, _ := shard.Snapshot()
	if err := shard.Publish(1, 10, []byte("old")); !errors.Is(err, ErrDurablePersistShardStale) {
		t.Fatalf("stale Publish() error = %v, want ErrDurablePersistShardStale", err)
	}
	after, _ := shard.Snapshot()
	if !bytes.Equal(before.Payload, after.Payload) || before.Generation != after.Generation || before.Upper != after.Upper {
		t.Fatalf("stale publish mutated state: before=%#v after=%#v", before, after)
	}
	encoded, err := shard.MarshalSnapshot()
	if err != nil {
		t.Fatalf("MarshalSnapshot() error = %v", err)
	}
	encoded[len(encoded)-1]++
	if err := shard.RestoreSnapshot(encoded); !errors.Is(err, ErrDurablePersistShardSnapshotInvalid) {
		t.Fatalf("corrupt RestoreSnapshot() error = %v, want invalid", err)
	}
	afterCorrupt, _ := shard.Snapshot()
	if !bytes.Equal(after.Payload, afterCorrupt.Payload) || afterCorrupt.Generation != after.Generation {
		t.Fatal("corrupt restore mutated state")
	}
}

func TestMZ001DurablePersistShardUsesAtomicFileStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "orders-eu.snapshot")
	store, err := NewFrontierSnapshotFileStore(FrontierSnapshotFileStoreOptions{Path: path, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("NewFrontierSnapshotFileStore() error = %v", err)
	}
	shard, err := NewDurablePersistShard(DurablePersistShardOptions{ShardID: "orders-eu"})
	if err != nil {
		t.Fatalf("NewDurablePersistShard() error = %v", err)
	}
	if err := shard.Publish(1, 3, []byte("file-state")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if err := shard.Save(context.Background(), store); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	restored, err := NewDurablePersistShard(DurablePersistShardOptions{ShardID: "orders-eu"})
	if err != nil {
		t.Fatalf("NewDurablePersistShard(restored) error = %v", err)
	}
	found, err := restored.Hydrate(context.Background(), store)
	if err != nil || !found {
		t.Fatalf("Hydrate(file) = found %v, error %v; want found", found, err)
	}
	got, _ := restored.Snapshot()
	if string(got.Payload) != "file-state" {
		t.Fatalf("file payload = %q, want file-state", got.Payload)
	}
}

func TestMZ001DurablePersistShardValidatesIdentityLimitsAndContext(t *testing.T) {
	if _, err := NewDurablePersistShard(DurablePersistShardOptions{ShardID: ""}); !errors.Is(err, ErrDurablePersistShardOptionsInvalid) {
		t.Fatalf("empty shard ID error = %v, want options invalid", err)
	}
	if _, err := NewDurablePersistShard(DurablePersistShardOptions{ShardID: "orders", MaxBytes: 1}); !errors.Is(err, ErrDurablePersistShardOptionsInvalid) {
		t.Fatalf("small max bytes error = %v, want options invalid", err)
	}
	shard, err := NewDurablePersistShard(DurablePersistShardOptions{ShardID: "orders-eu"})
	if err != nil {
		t.Fatalf("NewDurablePersistShard() error = %v", err)
	}
	if err := shard.Publish(1, 1, []byte("payload")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	encoded, err := shard.MarshalSnapshot()
	if err != nil {
		t.Fatalf("MarshalSnapshot() error = %v", err)
	}
	other, err := NewDurablePersistShard(DurablePersistShardOptions{ShardID: "orders-us"})
	if err != nil {
		t.Fatalf("NewDurablePersistShard(other) error = %v", err)
	}
	if err := other.RestoreSnapshot(encoded); !errors.Is(err, ErrDurablePersistShardSnapshotInvalid) {
		t.Fatalf("wrong shard restore error = %v, want invalid", err)
	}
	if _, ok := other.Snapshot(); ok {
		t.Fatal("wrong shard restore published state")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := shard.Save(canceled, new(mz001MemoryStore)); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Save() error = %v, want context.Canceled", err)
	}
	if found, err := other.Hydrate(canceled, new(mz001MemoryStore)); !errors.Is(err, context.Canceled) || found {
		t.Fatalf("canceled Hydrate() = found %v, error %v", found, err)
	}
}
