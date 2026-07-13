package hatriecache

import (
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestElectionStoreKeepsHealthyPrimaryAndPromotesReplica(t *testing.T) {
	topology := electionTestTopology(t)
	store := NewElectionStore(topology, ElectionOptions{})

	status := store.Status()
	if got := leaderByShard(status, 0); !got.Available || got.Leader != "node-a" {
		t.Fatalf("initial shard 0 leader = %#v, want node-a", got)
	}

	if err := store.MarkOffline("node-a"); err != nil {
		t.Fatalf("MarkOffline(node-a) error = %v", err)
	}
	route, ok := store.LeaderForKey(keyForShard(t, topology, 0))
	if !ok {
		t.Fatal("LeaderForKey() = false, want route")
	}
	if route.Leader.Leader != "node-b" || !route.Leader.Available {
		t.Fatalf("promoted route = %#v, want node-b leader", route)
	}
	if !reflect.DeepEqual(route.Leader.Candidates, []string{"node-a", "node-b"}) {
		t.Fatalf("candidates = %#v, want primary then replica", route.Leader.Candidates)
	}
}

func TestElectionStoreTimesOutHeartbeats(t *testing.T) {
	topology := electionTestTopology(t)
	now := time.Unix(1000, 0)
	store := NewElectionStore(topology, ElectionOptions{
		Timeout: time.Second,
		Now:     func() time.Time { return now },
	})

	if err := store.Heartbeat("node-a"); err != nil {
		t.Fatalf("Heartbeat(node-a) error = %v", err)
	}
	now = now.Add(2 * time.Second)

	status := store.Status()
	nodeA := nodeStatusByID(status, "node-a")
	if nodeA.Online || nodeA.Reason != "timeout" {
		t.Fatalf("node-a status = %#v, want timeout", nodeA)
	}
	if got := leaderByShard(status, 0); !got.Available || got.Leader != "node-b" {
		t.Fatalf("timed-out shard 0 leader = %#v, want node-b", got)
	}
}

func TestElectionStoreReportsUnavailableWhenAllCandidatesOffline(t *testing.T) {
	topology := electionTestTopology(t)
	store := NewElectionStore(topology, ElectionOptions{})

	if err := store.MarkOffline("node-a"); err != nil {
		t.Fatalf("MarkOffline(node-a) error = %v", err)
	}
	if err := store.MarkOffline("node-b"); err != nil {
		t.Fatalf("MarkOffline(node-b) error = %v", err)
	}

	status := store.Status()
	if got := leaderByShard(status, 0); got.Available || got.Leader != "" {
		t.Fatalf("shard 0 leader = %#v, want unavailable", got)
	}
}

func TestElectionStoreFullReplicaLeaderUsesSelfThenReplica(t *testing.T) {
	topology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Mode:    TopologyModeFullReplica,
		Self:    "node-b",
		Nodes: []TopologyNode{
			{ID: "node-a"},
			{ID: "node-b"},
		},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore(full replica) error = %v", err)
	}
	store := NewElectionStore(topology, ElectionOptions{})

	status := store.Status()
	if len(status.Leaders) != 1 || status.Leaders[0].Leader != "node-b" {
		t.Fatalf("full replica leaders = %#v, want node-b", status.Leaders)
	}
	if err := store.MarkOffline("node-b"); err != nil {
		t.Fatalf("MarkOffline(node-b) error = %v", err)
	}
	if got := store.Status().Leaders[0]; got.Leader != "node-a" || !got.Available {
		t.Fatalf("full replica promoted leader = %#v, want node-a", got)
	}
}

func TestElectionStoreRejectsUnknownNode(t *testing.T) {
	store := NewElectionStore(electionTestTopology(t), ElectionOptions{})
	if err := store.Heartbeat("missing"); err == nil {
		t.Fatal("Heartbeat(missing) error = nil, want error")
	}
	if err := store.MarkOffline(""); err == nil {
		t.Fatal("MarkOffline(empty) error = nil, want error")
	}
}

func electionTestTopology(t *testing.T) *TopologyStore {
	t.Helper()
	topology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Nodes: []TopologyNode{
			{ID: "node-a"},
			{ID: "node-b"},
			{ID: "node-c"},
		},
		Shards: []TopologyShard{
			{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}},
			{ID: 1, Primary: "node-c", Replicas: []string{"node-b"}},
		},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	return topology
}

func leaderByShard(status ElectionStatus, shard uint32) ElectionLeader {
	for _, leader := range status.Leaders {
		if leader.Shard == shard {
			return leader
		}
	}
	return ElectionLeader{}
}

func nodeStatusByID(status ElectionStatus, id string) ElectionNodeStatus {
	for _, node := range status.Nodes {
		if node.ID == id {
			return node
		}
	}
	return ElectionNodeStatus{}
}

func keyForShard(t *testing.T, topology *TopologyStore, shardID uint32) string {
	t.Helper()
	for idx := 0; idx < 10000; idx++ {
		key := "key:" + strconv.Itoa(idx)
		route, ok := topology.Route(key)
		if ok && route.Shard.ID == shardID {
			return key
		}
	}
	t.Fatalf("no key routed to shard %d", shardID)
	return ""
}
