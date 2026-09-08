package hatCache

import "testing"

func TestTopologyStorePartitionOwnershipSnapshot(t *testing.T) {
	store, err := NewTopologyStore(ClusterTopology{
		Version:      clusterTopologyVersion,
		Mode:         TopologyModeSharded,
		BucketCount:  8,
		FencingToken: 17,
		Nodes:        []TopologyNode{{ID: "node-a"}, {ID: "node-b"}},
		Shards:       []TopologyShard{{ID: 2, Primary: "node-a", Replicas: []string{"node-b"}}},
		BucketRanges: []TopologyBucketRange{{Start: 0, End: 7, Shard: 2}},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	ownership, ok := store.OwnershipForKey("key")
	if !ok || ownership.ShardID != 2 || ownership.FencingToken != 17 {
		t.Fatalf("OwnershipForKey() = %#v/%v", ownership, ok)
	}
	ownership.Replicas[0] = "mutated"
	current, ok := store.OwnershipForShard(2)
	if !ok || current.Replicas[0] != "node-b" {
		t.Fatalf("ownership snapshot was not independent: %#v/%v", current, ok)
	}
}

func TestTopologyStorePartitionOwnershipValidatesWrite(t *testing.T) {
	store, err := NewTopologyStore(ClusterTopology{
		Version:      clusterTopologyVersion,
		Mode:         TopologyModeSharded,
		FencingToken: 17,
		Nodes:        []TopologyNode{{ID: "node-a"}, {ID: "node-b"}},
		Shards:       []TopologyShard{{ID: 2, Primary: "node-a", Replicas: []string{"node-b"}}},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	ownership, ok := store.OwnershipForShard(2)
	if !ok {
		t.Fatal("OwnershipForShard() did not find the shard")
	}
	if err := store.ValidatePartitionOwnership(ownership); err != nil {
		t.Fatalf("ValidatePartitionOwnership() error = %v", err)
	}
	if err := store.ValidatePartitionWrite(ownership, "node-a", 17); err != nil {
		t.Fatalf("ValidatePartitionWrite() error = %v", err)
	}
	if err := store.ValidatePartitionWrite(ownership, "node-a", 16); err == nil {
		t.Fatal("ValidatePartitionWrite() accepted a stale fencing token")
	}
}
