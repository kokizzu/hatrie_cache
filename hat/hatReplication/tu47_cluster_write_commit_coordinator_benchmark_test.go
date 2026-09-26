package hatReplication_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatReplication"
)

func BenchmarkT047iClusterWriteCommitDirect(b *testing.B) {
	proposal := testClusterWriteCommitCoordinatorProposal()
	nodes := []string{"node-a", "node-b", "node-c"}
	prepare := func(context.Context, string, hatReplication.ClusterWriteCommitProposal) error { return nil }
	commit := func(context.Context, string, hatReplication.ClusterWriteCommitProposal) error { return nil }
	abort := func(context.Context, string, hatReplication.ClusterWriteCommitProposal) error { return nil }
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := hatReplication.ExecuteClusterWriteCommit(context.Background(), nodes, proposal, prepare, commit, abort); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkT047iClusterWriteCommitMemoryStateStore(b *testing.B) {
	proposal := testClusterWriteCommitCoordinatorProposal()
	nodes := []string{"node-a", "node-b", "node-c"}
	prepare := func(context.Context, string, hatReplication.ClusterWriteCommitProposal) error { return nil }
	commit := func(context.Context, string, hatReplication.ClusterWriteCommitProposal) error { return nil }
	abort := func(context.Context, string, hatReplication.ClusterWriteCommitProposal) error { return nil }
	store := &benchmarkClusterWriteCommitCoordinatorStateStore{}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := hatReplication.ExecuteClusterWriteCommitWithStateStore(context.Background(), nodes, proposal, prepare, commit, abort, store); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkT047iClusterWriteCommitFileStoreSaveLoad(b *testing.B) {
	store, err := hatReplication.NewClusterWriteCommitCoordinatorFileStore(hatReplication.ClusterWriteCommitCoordinatorFileStoreOptions{
		Path: b.TempDir() + "/coordinator.bin",
	})
	if err != nil {
		b.Fatal(err)
	}
	snapshot := hatReplication.ClusterWriteCommitCoordinatorSnapshot{
		Proposal: testClusterWriteCommitCoordinatorProposal(),
		Nodes:    []string{"node-a", "node-b", "node-c"},
		Attempts: []hatReplication.ClusterWriteCommitAttempt{
			{Node: "node-a", Prepared: true, Committed: true},
			{Node: "node-b", Prepared: true, Committed: true},
			{Node: "node-c", Prepared: true, Committed: true},
		},
		Phase: hatReplication.ClusterWriteCommitCoordinatorCommitted,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := store.Save(context.Background(), snapshot); err != nil {
			b.Fatal(err)
		}
		if _, found, err := store.Load(context.Background()); err != nil || !found {
			b.Fatalf("Load() found=%v error=%v", found, err)
		}
	}
}

func BenchmarkT047iClusterWriteCommitFileStateStore(b *testing.B) {
	store, err := hatReplication.NewClusterWriteCommitCoordinatorFileStore(hatReplication.ClusterWriteCommitCoordinatorFileStoreOptions{
		Path: b.TempDir() + "/coordinator.bin",
	})
	if err != nil {
		b.Fatal(err)
	}
	proposal := testClusterWriteCommitCoordinatorProposal()
	nodes := []string{"node-a", "node-b", "node-c"}
	prepare := func(context.Context, string, hatReplication.ClusterWriteCommitProposal) error { return nil }
	commit := func(context.Context, string, hatReplication.ClusterWriteCommitProposal) error { return nil }
	abort := func(context.Context, string, hatReplication.ClusterWriteCommitProposal) error { return nil }
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := hatReplication.ExecuteClusterWriteCommitWithStateStore(context.Background(), nodes, proposal, prepare, commit, abort, store); err != nil {
			b.Fatal(err)
		}
	}
}

type benchmarkClusterWriteCommitCoordinatorStateStore struct{}

func (*benchmarkClusterWriteCommitCoordinatorStateStore) Save(context.Context, hatReplication.ClusterWriteCommitCoordinatorSnapshot) error {
	return nil
}
