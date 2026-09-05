package hatTopology

import (
	"path/filepath"
	"testing"
)

func TestClusterTopologyFingerprintIncludesFencingToken(t *testing.T) {
	base := ClusterTopology{
		Version: 1,
		Mode:    TopologyModeSharded,
		Nodes: []TopologyNode{
			{ID: "node-a"},
			{ID: "node-b"},
		},
		Shards: []TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
	}
	withToken := base
	withToken.FencingToken = 42
	if base.Fingerprint() == withToken.Fingerprint() {
		t.Fatalf("fingerprint without and with fencing token are equal: %q", base.Fingerprint())
	}
}

func TestTopologyFencingTokenPersistsInJSON(t *testing.T) {
	topology := ClusterTopology{
		Version:      1,
		Mode:         TopologyModeFullReplica,
		FencingToken: 42,
		Nodes:        []TopologyNode{{ID: "node-a"}},
	}
	path := filepath.Join(t.TempDir(), "topology.json")
	if err := SaveTopology(path, topology); err != nil {
		t.Fatalf("SaveTopology() error = %v", err)
	}
	loaded, err := LoadTopology(path)
	if err != nil {
		t.Fatalf("LoadTopology() error = %v", err)
	}
	if loaded.FencingToken != topology.FencingToken {
		t.Fatalf("loaded fencing token = %d, want %d", loaded.FencingToken, topology.FencingToken)
	}
}
