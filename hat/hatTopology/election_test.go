package hatTopology_test

import (
	"reflect"
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

func TestElectionStoreReportsAndPrunesOrphanNodes(t *testing.T) {
	provider := &staticTopologyProvider{topology: hatTopology.ClusterTopology{
		Version: 1,
		Mode:    hatTopology.TopologyModeSharded,
		Nodes: []hatTopology.TopologyNode{
			{ID: "node-a"},
			{ID: "node-b"},
		},
		Shards: []hatTopology.TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
	}}
	store := hatTopology.NewElectionStore(provider, hatTopology.ElectionOptions{})
	if err := store.Heartbeat("node-a"); err != nil {
		t.Fatalf("Heartbeat(node-a) error = %v", err)
	}
	if err := store.MarkOffline("node-b"); err != nil {
		t.Fatalf("MarkOffline(node-b) error = %v", err)
	}
	if !store.IsHealthy("node-a") {
		t.Fatal("IsHealthy(node-a) = false, want true")
	}
	if store.IsHealthy("node-b") {
		t.Fatal("IsHealthy(node-b) = true, want false")
	}

	provider.topology = hatTopology.ClusterTopology{
		Version: 2,
		Mode:    hatTopology.TopologyModeSharded,
		Nodes:   []hatTopology.TopologyNode{{ID: "node-c"}},
	}
	want := []string{"node-a", "node-b"}
	if got := store.OrphanNodes(); !reflect.DeepEqual(got, want) {
		t.Fatalf("OrphanNodes() = %#v, want %#v", got, want)
	}
	if got := store.Status().OrphanNodes; !reflect.DeepEqual(got, want) {
		t.Fatalf("Status().OrphanNodes = %#v, want %#v", got, want)
	}
	if store.IsHealthy("node-a") {
		t.Fatal("IsHealthy(orphan node-a) = true, want false")
	}
	if !store.IsHealthy("node-c") {
		t.Fatal("IsHealthy(untracked current node-c) = false, want true")
	}
	if got := store.PruneOrphanNodes(); !reflect.DeepEqual(got, want) {
		t.Fatalf("PruneOrphanNodes() = %#v, want %#v", got, want)
	}
	if got := store.OrphanNodes(); len(got) != 0 {
		t.Fatalf("OrphanNodes() after prune = %#v, want empty", got)
	}
	if got := store.PruneOrphanNodes(); len(got) != 0 {
		t.Fatalf("second PruneOrphanNodes() = %#v, want empty", got)
	}
}
