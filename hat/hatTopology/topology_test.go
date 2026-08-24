package hatTopology_test

import (
	"path/filepath"
	"testing"

	"hatrie_cache/hat/hatTopology"
)

func TestTopologyModelNormalizesRoutesAndPersists(t *testing.T) {
	topology, err := hatTopology.Normalize(hatTopology.ClusterTopology{
		Mode: "sharded",
		Self: "node-b",
		Nodes: []hatTopology.TopologyNode{
			{ID: "node-b", Address: "127.0.0.1:9002"},
			{ID: "node-a", Address: "127.0.0.1:9001"},
		},
		Shards: []hatTopology.TopologyShard{
			{ID: 1, Primary: "node-b", Replicas: []string{"node-a"}},
		},
	})
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if topology.Version != hatTopology.Version {
		t.Fatalf("Version = %d, want %d", topology.Version, hatTopology.Version)
	}
	if topology.Nodes[0].ID != "node-a" {
		t.Fatalf("nodes not normalized: %+v", topology.Nodes)
	}

	route, ok := topology.RouteForKey("customer:42")
	if !ok {
		t.Fatal("RouteForKey() ok = false")
	}
	if route.Shard.Primary != "node-b" || len(route.Owners) != 2 {
		t.Fatalf("RouteForKey() = %+v, want node-b with two owners", route)
	}

	path := filepath.Join(t.TempDir(), "topology.json")
	if err := hatTopology.SaveTopology(path, topology); err != nil {
		t.Fatalf("SaveTopology() error = %v", err)
	}
	loaded, err := hatTopology.LoadTopology(path)
	if err != nil {
		t.Fatalf("LoadTopology() error = %v", err)
	}
	if loaded.Fingerprint() != topology.Fingerprint() {
		t.Fatalf("loaded fingerprint = %q, want %q", loaded.Fingerprint(), topology.Fingerprint())
	}
}
