package hatTopology_test

import (
	"testing"
	"time"

	"hatrie_cache/hat/hatTopology"
)

type staticTopologyProvider struct {
	topology hatTopology.ClusterTopology
}

func (provider staticTopologyProvider) TopologySnapshot() hatTopology.ClusterTopology {
	return provider.topology
}

func TestElectionStorePromotesReplicaAndReportsStatus(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	provider := staticTopologyProvider{topology: hatTopology.ClusterTopology{
		Version: 1,
		Mode:    hatTopology.TopologyModeSharded,
		Nodes: []hatTopology.TopologyNode{
			{ID: "node-a"},
			{ID: "node-b"},
		},
		Shards: []hatTopology.TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
	}}
	store := hatTopology.NewElectionStore(provider, hatTopology.ElectionOptions{
		Timeout: time.Second,
		Now:     func() time.Time { return now },
	})

	route, ok := store.LeaderForKey("account:42")
	if !ok || !route.Leader.Available || route.Leader.Leader != "node-a" {
		t.Fatalf("initial LeaderForKey() = %#v, %v; want node-a", route, ok)
	}
	if err := store.MarkOffline("node-a"); err != nil {
		t.Fatalf("MarkOffline() error = %v", err)
	}

	route, ok = store.LeaderForKey("account:42")
	if !ok || !route.Leader.Available || route.Leader.Leader != "node-b" {
		t.Fatalf("failover LeaderForKey() = %#v, %v; want node-b", route, ok)
	}
	status := store.Status()
	if len(status.Nodes) != 2 || status.Nodes[0].ID != "node-a" || status.Nodes[0].Reason != "offline" {
		t.Fatalf("Status().Nodes = %#v; want node-a offline", status.Nodes)
	}
}
