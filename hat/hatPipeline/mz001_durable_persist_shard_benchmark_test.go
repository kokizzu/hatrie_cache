package hatPipeline

import (
	"context"
	"testing"
)

var mz001PersistShardBenchmarkSink any

func benchmarkMZ001PersistShard(t testing.TB) (*DurablePersistShard, *mz001MemoryStore) {
	t.Helper()
	shard, err := NewDurablePersistShard(DurablePersistShardOptions{ShardID: "orders-eu", MaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("NewDurablePersistShard() error = %v", err)
	}
	payload := make([]byte, 128<<10)
	for index := range payload {
		payload[index] = byte(index * 31)
	}
	if err := shard.Publish(4096, 4096, payload); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	store := new(mz001MemoryStore)
	if err := shard.Save(context.Background(), store); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	return shard, store
}

func BenchmarkMZ001DurablePersistShardMarshal(b *testing.B) {
	shard, _ := benchmarkMZ001PersistShard(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		encoded, err := shard.MarshalSnapshot()
		if err != nil {
			b.Fatal(err)
		}
		mz001PersistShardBenchmarkSink = encoded
	}
}

func BenchmarkMZ001DurablePersistShardHydrate(b *testing.B) {
	_, store := benchmarkMZ001PersistShard(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		shard, err := NewDurablePersistShard(DurablePersistShardOptions{ShardID: "orders-eu", MaxBytes: 1 << 20})
		if err != nil {
			b.Fatal(err)
		}
		found, err := shard.Hydrate(context.Background(), store)
		if err != nil || !found {
			b.Fatalf("Hydrate() = found %v, error %v", found, err)
		}
		mz001PersistShardBenchmarkSink = shard
	}
}
