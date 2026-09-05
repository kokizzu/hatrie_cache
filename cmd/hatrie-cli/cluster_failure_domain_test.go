package main

import (
	"strings"
	"testing"

	hatriecache "hatrie_cache/hat/hatTopology"
)

func TestClusterReplicaPlacementHonorsFailureDomains(t *testing.T) {
	topology := hatriecache.ClusterTopology{
		Version: hatriecache.Version,
		Mode:    hatriecache.TopologyModeSharded,
		Nodes: []hatriecache.TopologyNode{
			{ID: "node-a", Address: "http://node-a", Role: "primary", FailureDomain: "zone-a"},
			{ID: "node-b", Address: "http://node-b", Role: "replica", FailureDomain: "zone-b"},
		},
		Shards: []hatriecache.TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
	}

	updated, changed, replaced, err := clusterAddReplicaTopologyWithPlacement(topology, "node-c", "http://node-c", false, "zone-c", 3)
	if err != nil {
		t.Fatalf("clusterAddReplicaTopologyWithPlacement() error = %v", err)
	}
	if !changed || replaced || len(updated.Shards) != 1 || !stringInSlice(updated.Shards[0].Replicas, "node-c") {
		t.Fatalf("placement result = changed %v replaced %v topology %#v", changed, replaced, updated)
	}
	for _, node := range updated.Nodes {
		if node.ID == "node-c" && node.FailureDomain != "zone-c" {
			t.Fatalf("new replica failure domain = %q, want zone-c", node.FailureDomain)
		}
	}

	if _, _, _, err := clusterAddReplicaTopologyWithPlacement(topology, "node-c", "http://node-c", false, "zone-a", 3); err == nil || !strings.Contains(err.Error(), "distinct failure domains") {
		t.Fatalf("duplicate-domain placement error = %v, want distinct-domain rejection", err)
	}
	if _, _, _, err := clusterAddReplicaTopologyWithPlacement(topology, "node-c", "http://node-c", false, "", 3); err == nil || !strings.Contains(err.Error(), "failure domain is required") {
		t.Fatalf("unknown-domain placement error = %v, want required-domain rejection", err)
	}
}
