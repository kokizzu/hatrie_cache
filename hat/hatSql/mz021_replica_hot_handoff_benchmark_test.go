package hatSql

import (
	"context"
	"sync/atomic"
	"testing"
)

var mz021ReplicaHotHandoffBenchmarkSink atomic.Uint64

type mz021ReplicaHotHandoffBenchmarkTarget struct{}

func (mz021ReplicaHotHandoffBenchmarkTarget) InstallSnapshot(context.Context, ReplicaHotHandoffSnapshot) error {
	return nil
}

func (mz021ReplicaHotHandoffBenchmarkTarget) Apply(context.Context, ReplicaHotHandoffBatch) error {
	mz021ReplicaHotHandoffBenchmarkSink.Add(1)
	return nil
}

func (mz021ReplicaHotHandoffBenchmarkTarget) Ready(context.Context) error {
	return nil
}

type mz021ReplicaHotHandoffBenchmarkSource struct {
	snapshot ReplicaHotHandoffSnapshot
	batch    ReplicaHotHandoffBatch
}

func (source mz021ReplicaHotHandoffBenchmarkSource) Snapshot(context.Context) (ReplicaHotHandoffSnapshot, error) {
	snapshot := source.snapshot
	snapshot.Payload = append([]byte(nil), snapshot.Payload...)
	return snapshot, nil
}

func (source mz021ReplicaHotHandoffBenchmarkSource) Fetch(_ context.Context, _ uint64, _ int) (ReplicaHotHandoffBatch, error) {
	return source.batch, nil
}

func BenchmarkReplicaHotHandoffWarmTail(b *testing.B) {
	source := mz021ReplicaHotHandoffBenchmarkSource{
		snapshot: ReplicaHotHandoffSnapshot{
			SourceID:   "primary",
			SnapshotID: "benchmark-snapshot",
			Epoch:      1,
			Frontier:   1000,
			Payload:    make([]byte, 64<<10),
		},
		batch: ReplicaHotHandoffBatch{
			Epoch:          1,
			SourceFrontier: 1001,
			Deltas:         []ReplicaHotHandoffDelta{{Sequence: 1001, Payload: []byte("tail")}},
		},
	}
	target := mz021ReplicaHotHandoffBenchmarkTarget{}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		handoff, err := NewReplicaHotHandoff(ReplicaHotHandoffOptions{SourceID: "primary", TargetID: "replica-b", FencingToken: 1, PollInterval: 1})
		if err != nil {
			b.Fatal(err)
		}
		if err := handoff.Prepare(context.Background(), source, target); err != nil {
			b.Fatal(err)
		}
		if err := handoff.CatchUp(context.Background(), source, target); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReplicaHotHandoffDirectTailControl(b *testing.B) {
	batch := ReplicaHotHandoffBatch{
		Epoch:          1,
		SourceFrontier: 1001,
		Deltas:         []ReplicaHotHandoffDelta{{Sequence: 1001, Payload: []byte("tail")}},
	}
	target := mz021ReplicaHotHandoffBenchmarkTarget{}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := target.Apply(context.Background(), batch); err != nil {
			b.Fatal(err)
		}
	}
}
