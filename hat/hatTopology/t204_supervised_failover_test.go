package hatTopology_test

import (
	"strings"
	"testing"

	"hatrie_cache/hat/hatTopology"
)

type t204TopologyProvider struct {
	topology hatTopology.ClusterTopology
}

func (provider t204TopologyProvider) TopologySnapshot() hatTopology.ClusterTopology {
	return provider.topology
}

func newT204ElectionStore() *hatTopology.ElectionStore {
	provider := t204TopologyProvider{topology: hatTopology.ClusterTopology{
		Version: 1,
		Mode:    hatTopology.TopologyModeSharded,
		Nodes: []hatTopology.TopologyNode{
			{ID: "node-a"},
			{ID: "node-b"},
		},
		Shards: []hatTopology.TopologyShard{{
			ID:       0,
			Primary:  "node-a",
			Replicas: []string{"node-b"},
		}},
	}}
	store := hatTopology.NewElectionStore(provider, hatTopology.ElectionOptions{
		RequireHeartbeat: true,
	})
	if err := store.Heartbeat("node-a"); err != nil {
		panic(err)
	}
	if err := store.Heartbeat("node-b"); err != nil {
		panic(err)
	}
	return store
}

func TestElectionStoreOperatorOverrideControlsLeaderUntilRecoveryCompletes(t *testing.T) {
	store := newT204ElectionStore()
	if err := store.SetLeaderOverride(0, "node-b", "primary maintenance"); err != nil {
		t.Fatalf("SetLeaderOverride() error = %v", err)
	}

	route, ok := store.LeaderForKey("account:42")
	if !ok || route.Leader.Leader != "node-b" || !route.Leader.Available {
		t.Fatalf("overridden LeaderForKey() = %#v, %v; want available node-b", route, ok)
	}
	status := store.Status()
	if len(status.Controls) != 1 || status.Controls[0].State != hatTopology.ElectionRecoveryOperatorOverride {
		t.Fatalf("override status = %#v; want operator override", status.Controls)
	}

	if err := store.BeginRecovery(0, "primary restored"); err != nil {
		t.Fatalf("BeginRecovery() error = %v", err)
	}
	if err := store.Heartbeat("node-a"); err != nil {
		t.Fatalf("Heartbeat(node-a) error = %v", err)
	}
	route, ok = store.LeaderForKey("account:42")
	if !ok || route.Leader.Leader != "node-b" {
		t.Fatalf("recovery LeaderForKey() = %#v, %v; want controlled node-b", route, ok)
	}
	status = store.Status()
	if len(status.Controls) != 1 || status.Controls[0].State != hatTopology.ElectionRecoveryInProgress {
		t.Fatalf("recovery status = %#v; want recovery", status.Controls)
	}

	if err := store.CompleteRecovery(0); err != nil {
		t.Fatalf("CompleteRecovery() error = %v", err)
	}
	route, ok = store.LeaderForKey("account:42")
	if !ok || route.Leader.Leader != "node-a" || !route.Leader.Available {
		t.Fatalf("automatic LeaderForKey() = %#v, %v; want available node-a", route, ok)
	}
	if status = store.Status(); len(status.Controls) != 0 {
		t.Fatalf("completed recovery controls = %#v; want none", status.Controls)
	}
}

func TestElectionStoreSupervisedLeaderDoesNotFallbackWhenUnavailable(t *testing.T) {
	store := newT204ElectionStore()
	if err := store.SetLeaderOverride(0, "node-b", "operator promotion"); err != nil {
		t.Fatalf("SetLeaderOverride() error = %v", err)
	}
	if err := store.MarkOffline("node-b"); err != nil {
		t.Fatalf("MarkOffline(node-b) error = %v", err)
	}

	route, ok := store.LeaderForKey("account:42")
	if !ok || route.Leader.Leader != "node-b" || route.Leader.Available {
		t.Fatalf("supervised unavailable LeaderForKey() = %#v, %v; want unavailable node-b", route, ok)
	}
}

func TestElectionStoreCompleteRecoveryRequiresHealthyControlledLeader(t *testing.T) {
	store := newT204ElectionStore()
	if err := store.SetLeaderOverride(0, "node-b", "operator promotion"); err != nil {
		t.Fatalf("SetLeaderOverride() error = %v", err)
	}
	if err := store.BeginRecovery(0, "primary restored"); err != nil {
		t.Fatalf("BeginRecovery() error = %v", err)
	}
	if err := store.MarkOffline("node-b"); err != nil {
		t.Fatalf("MarkOffline(node-b) error = %v", err)
	}
	if err := store.CompleteRecovery(0); err == nil || !strings.Contains(err.Error(), "not healthy") {
		t.Fatalf("CompleteRecovery() error = %v; want healthy-leader validation", err)
	}
	status := store.Status()
	if len(status.Controls) != 1 || status.Controls[0].State != hatTopology.ElectionRecoveryInProgress {
		t.Fatalf("failed recovery status = %#v; want recovery control retained", status.Controls)
	}
}

func TestElectionStoreSupervisedFailoverValidatesLifecycle(t *testing.T) {
	store := newT204ElectionStore()
	if err := store.SetLeaderOverride(0, "not-an-owner", "bad operator input"); err == nil || !strings.Contains(err.Error(), "not an owner") {
		t.Fatalf("invalid override error = %v; want owner validation", err)
	}
	if err := store.BeginRecovery(0, "without override"); err == nil || !strings.Contains(err.Error(), "override") {
		t.Fatalf("BeginRecovery() error = %v; want override requirement", err)
	}
	if err := store.CompleteRecovery(0); err == nil || !strings.Contains(err.Error(), "recovery") {
		t.Fatalf("CompleteRecovery() error = %v; want recovery requirement", err)
	}
	if err := store.SetLeaderOverride(0, "node-b", "operator promotion"); err != nil {
		t.Fatalf("SetLeaderOverride() error = %v", err)
	}
	if err := store.CompleteRecovery(0); err == nil || !strings.Contains(err.Error(), "recovery") {
		t.Fatalf("CompleteRecovery() before BeginRecovery error = %v; want recovery requirement", err)
	}
}
