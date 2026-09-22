package hatTopology_test

import (
	"context"
	"testing"
	"time"

	"hatrie_cache/hat/hatTopology"
)

func TestElectionStoreRunAutomaticallyPromotesLiveReplica(t *testing.T) {
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
		Timeout:          40 * time.Millisecond,
		RequireHeartbeat: true,
	})

	if route, ok := store.LeaderForKey("account:42"); !ok || route.Leader.Available {
		t.Fatalf("LeaderForKey() before heartbeats = %#v, %v; want no available leader", route, ok)
	}

	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelA()
	defer cancelB()
	doneA := make(chan error, 1)
	doneB := make(chan error, 1)
	go func() { doneA <- store.Run(ctxA, "node-a", 5*time.Millisecond) }()
	go func() { doneB <- store.Run(ctxB, "node-b", 5*time.Millisecond) }()

	waitForT202Leader(t, store, "node-a")
	cancelA()
	if err := <-doneA; err != nil {
		t.Fatalf("Run(node-a) error = %v", err)
	}
	waitForT202Leader(t, store, "node-b")
	cancelB()
	if err := <-doneB; err != nil {
		t.Fatalf("Run(node-b) error = %v", err)
	}
}

func TestElectionStoreRunDoesNotHeartbeatCanceledContext(t *testing.T) {
	provider := staticTopologyProvider{topology: hatTopology.ClusterTopology{
		Version: 1,
		Mode:    hatTopology.TopologyModeSharded,
		Nodes: []hatTopology.TopologyNode{
			{ID: "node-a"},
		},
		Shards: []hatTopology.TopologyShard{{ID: 0, Primary: "node-a"}},
	}}
	store := hatTopology.NewElectionStore(provider, hatTopology.ElectionOptions{
		RequireHeartbeat: true,
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := store.Run(ctx, "node-a", time.Millisecond); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if store.IsHealthy("node-a") {
		t.Fatal("Run() recorded a heartbeat after context cancellation")
	}
}

func waitForT202Leader(t *testing.T, store *hatTopology.ElectionStore, want string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if route, ok := store.LeaderForKey("account:42"); ok && route.Leader.Available && route.Leader.Leader == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	route, ok := store.LeaderForKey("account:42")
	t.Fatalf("LeaderForKey() = %#v, %v; want %q", route, ok, want)
}
