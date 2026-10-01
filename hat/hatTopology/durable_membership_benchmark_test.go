package hatTopology

import (
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkSaveTopologyExistingControl(b *testing.B) {
	path := filepath.Join(b.TempDir(), "topology.json")
	topology := ClusterTopology{
		Mode:  TopologyModeFullReplica,
		Self:  "node-a",
		Nodes: []TopologyNode{{ID: "node-a", Address: "127.0.0.1:9001", Role: "primary"}},
	}
	b.ReportAllocs()
	for range b.N {
		b.StopTimer()
		_ = os.Remove(path)
		if err := SaveTopology(path, topology); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if err := SaveTopology(path, topology); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDurableMembershipApply(b *testing.B) {
	path := filepath.Join(b.TempDir(), "membership.json")
	initial := ClusterTopology{
		Mode:  TopologyModeFullReplica,
		Self:  "node-a",
		Nodes: []TopologyNode{{ID: "node-a", Address: "127.0.0.1:9001", Role: "primary"}},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		_ = os.Remove(path)
		store, err := OpenDurableMembershipStore(path, initial)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if _, err := store.Apply(MembershipChange{
			ID:           "join-node-b",
			Kind:         MembershipJoin,
			FencingToken: 1,
			Node:         TopologyNode{ID: "node-b", Address: "127.0.0.1:9002", Role: "replica"},
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDurableMembershipSnapshot(b *testing.B) {
	path := filepath.Join(b.TempDir(), "membership.json")
	store, err := OpenDurableMembershipStore(path, ClusterTopology{
		Mode: TopologyModeFullReplica,
		Self: "node-a",
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "127.0.0.1:9001", Role: "primary"},
			{ID: "node-b", Address: "127.0.0.1:9002", Role: "replica"},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		snapshot := store.Snapshot()
		if snapshot.Generation != 0 || len(snapshot.Topology.Nodes) != 2 {
			b.Fatal(snapshot)
		}
	}
}
