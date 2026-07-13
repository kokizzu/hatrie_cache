package hatriecache

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestTopologyStoreValidatesNormalizesAndRoutes(t *testing.T) {
	topology := ClusterTopology{
		Version: 1,
		Self:    "node-b",
		Nodes: []TopologyNode{
			{ID: "node-b", Address: "127.0.0.1:8081", Role: "replica"},
			{ID: "node-a", Address: "127.0.0.1:8080", Role: "primary"},
		},
		Shards: []TopologyShard{
			{ID: 1, Primary: "node-b", Replicas: []string{"node-a"}},
			{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}},
		},
	}
	store, err := NewTopologyStore(topology)
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}

	got := store.Get()
	if got.Nodes[0].ID != "node-a" || got.Shards[0].ID != 0 {
		t.Fatalf("normalized topology = %#v, want sorted nodes and shards", got)
	}
	route, ok := store.Route("session:1")
	if !ok {
		t.Fatal("Route(session:1) = false, want true")
	}
	wantShard, ok := got.ShardForKey("session:1")
	if !ok || !reflect.DeepEqual(route.Shard, wantShard) {
		t.Fatalf("route = %#v, want shard %#v", route, wantShard)
	}

	got.Nodes[0].ID = "mutated"
	if store.Get().Nodes[0].ID != "node-a" {
		t.Fatal("TopologyStore.Get exposed internal node slice")
	}
}

func TestTopologyStoreRejectsInvalidTopology(t *testing.T) {
	for name, topology := range map[string]ClusterTopology{
		"no nodes":        {Version: 1, Shards: []TopologyShard{{ID: 0, Primary: "node-a"}}},
		"no shards":       {Version: 1, Nodes: []TopologyNode{{ID: "node-a"}}},
		"missing primary": {Version: 1, Nodes: []TopologyNode{{ID: "node-a"}}, Shards: []TopologyShard{{ID: 0, Primary: "missing"}}},
		"duplicate node": {
			Version: 1,
			Nodes:   []TopologyNode{{ID: "node-a"}, {ID: "node-a"}},
			Shards:  []TopologyShard{{ID: 0, Primary: "node-a"}},
		},
		"bad self": {
			Version: 1,
			Self:    "missing",
			Nodes:   []TopologyNode{{ID: "node-a"}},
			Shards:  []TopologyShard{{ID: 0, Primary: "node-a"}},
		},
	} {
		if _, err := NewTopologyStore(topology); err == nil {
			t.Fatalf("NewTopologyStore(%s) error = nil, want error", name)
		}
	}
}

func TestTopologyStorePersistsToDisk(t *testing.T) {
	path := filepath.Join(t.TempDir(), "topology.json")
	topology := SingleNodeTopology("node-a", "127.0.0.1:8080")
	if err := SaveTopology(path, topology); err != nil {
		t.Fatalf("SaveTopology() error = %v", err)
	}

	loaded, err := LoadTopology(path)
	if err != nil {
		t.Fatalf("LoadTopology() error = %v", err)
	}
	if !reflect.DeepEqual(loaded, topology) {
		t.Fatalf("loaded topology = %#v, want %#v", loaded, topology)
	}

	store, err := OpenTopologyStore(path, SingleNodeTopology("fallback", ""))
	if err != nil {
		t.Fatalf("OpenTopologyStore() error = %v", err)
	}
	updated := ClusterTopology{
		Version: 1,
		Self:    "node-b",
		Nodes:   []TopologyNode{{ID: "node-b", Address: "127.0.0.1:8081"}},
		Shards:  []TopologyShard{{ID: 0, Primary: "node-b"}},
	}
	if err := store.Set(updated); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	reloaded, err := LoadTopology(path)
	if err != nil {
		t.Fatalf("LoadTopology(updated) error = %v", err)
	}
	if !reflect.DeepEqual(reloaded, store.Get()) {
		t.Fatalf("reloaded topology = %#v, want store topology %#v", reloaded, store.Get())
	}
}
