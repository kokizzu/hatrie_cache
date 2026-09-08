package hatTopology

import (
	"reflect"
	"strings"
	"testing"
)

var partitionOwnershipBenchmarkSink PartitionOwnership

func TestPartitionOwnershipPreservesLegacyTopologyDecoding(t *testing.T) {
	topology, err := DecodeJSON(strings.NewReader(`{"version":1,"mode":"sharded","nodes":[{"id":"node-a","address":"a"}],"shards":[{"id":0,"primary":"node-a"}]}`))
	if err != nil {
		t.Fatalf("DecodeJSON() error = %v", err)
	}
	ownership, ok := topology.OwnershipForShard(0)
	if !ok || ownership.Primary != "node-a" || ownership.TopologyFingerprint == "" {
		t.Fatalf("legacy ownership = %#v/%v", ownership, ok)
	}
}

func TestPartitionOwnershipForShardAndKey(t *testing.T) {
	topology := ClusterTopology{
		Version:      Version,
		Mode:         TopologyModeSharded,
		BucketCount:  16,
		FencingToken: 42,
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "a"},
			{ID: "node-b", Address: "b"},
		},
		Shards:       []TopologyShard{{ID: 7, Primary: "node-a", Replicas: []string{"node-b"}}},
		BucketRanges: []TopologyBucketRange{{Start: 0, End: 15, Shard: 7}},
	}

	ownership, ok := topology.OwnershipForShard(7)
	if !ok {
		t.Fatal("OwnershipForShard() did not find the shard")
	}
	if ownership.ShardID != 7 || ownership.Primary != "node-a" || ownership.FencingToken != 42 {
		t.Fatalf("ownership = %#v, want shard 7/node-a/token 42", ownership)
	}
	if ownership.TopologyFingerprint != topology.Fingerprint() {
		t.Fatalf("ownership fingerprint = %q, want %q", ownership.TopologyFingerprint, topology.Fingerprint())
	}
	if got := ownership.Owners(); !reflect.DeepEqual(got, []string{"node-a", "node-b"}) {
		t.Fatalf("Owners() = %#v", got)
	}
	if !ownership.IsPrimary("node-a") || ownership.IsPrimary("node-b") || !ownership.IsOwner("node-b") {
		t.Fatalf("owner predicates are incorrect: %#v", ownership)
	}

	byKey, ok := topology.OwnershipForKey("any-key")
	if !ok || byKey.ShardID != ownership.ShardID || byKey.TopologyFingerprint != ownership.TopologyFingerprint {
		t.Fatalf("OwnershipForKey() = %#v/%v, want %#v/true", byKey, ok, ownership)
	}

	owners := ownership.Owners()
	owners[0] = "mutated"
	if ownership.Primary != "node-a" {
		t.Fatalf("Owners() exposed mutable primary backing: %#v", ownership)
	}
}

func TestPartitionOwnershipForFullReplica(t *testing.T) {
	topology := ClusterTopology{
		Version: Version, Mode: TopologyModeFullReplica, Self: "node-b", FencingToken: 9,
		Nodes: []TopologyNode{{ID: "node-a"}, {ID: "node-b"}},
	}

	ownership, ok := topology.OwnershipForShard(0)
	if !ok || ownership.Primary != "node-b" || !reflect.DeepEqual(ownership.Replicas, []string{"node-a"}) {
		t.Fatalf("full replica ownership = %#v/%v", ownership, ok)
	}
	if _, ok := topology.OwnershipForShard(1); ok {
		t.Fatal("full replica topology returned ownership for a non-existent shard")
	}
}

func TestPartitionOwnershipValidatesSnapshotAndWrite(t *testing.T) {
	topology := ClusterTopology{
		Version: Version, Mode: TopologyModeSharded, FencingToken: 42,
		Nodes:  []TopologyNode{{ID: "node-a"}, {ID: "node-b"}},
		Shards: []TopologyShard{{ID: 3, Primary: "node-a", Replicas: []string{"node-b"}}},
	}
	ownership, ok := topology.OwnershipForShard(3)
	if !ok {
		t.Fatal("OwnershipForShard() did not find the shard")
	}
	if err := topology.ValidatePartitionOwnership(ownership); err != nil {
		t.Fatalf("ValidatePartitionOwnership() error = %v", err)
	}
	if err := topology.ValidatePartitionWrite(ownership, "node-a", 42); err != nil {
		t.Fatalf("ValidatePartitionWrite() error = %v", err)
	}

	cases := []struct {
		name       string
		ownership  PartitionOwnership
		nodeID     string
		fenceToken uint64
	}{
		{name: "stale fence", ownership: ownership, nodeID: "node-a", fenceToken: 41},
		{name: "replica write", ownership: ownership, nodeID: "node-b", fenceToken: 42},
		{name: "stale snapshot", ownership: func() PartitionOwnership {
			stale := ownership
			stale.TopologyFingerprint = "stale"
			return stale
		}(), nodeID: "node-a", fenceToken: 42},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := topology.ValidatePartitionWrite(tc.ownership, tc.nodeID, tc.fenceToken); err == nil {
				t.Fatalf("ValidatePartitionWrite() accepted invalid %s", tc.name)
			}
		})
	}
}

func BenchmarkPartitionOwnershipForShard(b *testing.B) {
	topology := ClusterTopology{
		Version:      Version,
		Mode:         TopologyModeSharded,
		FencingToken: 42,
		Nodes:        []TopologyNode{{ID: "node-a"}, {ID: "node-b"}},
		Shards:       []TopologyShard{{ID: 3, Primary: "node-a", Replicas: []string{"node-b"}}},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		ownership, ok := topology.OwnershipForShard(3)
		if !ok {
			b.Fatal("OwnershipForShard() did not find the shard")
		}
		partitionOwnershipBenchmarkSink = ownership
	}
}
