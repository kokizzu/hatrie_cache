package hatriecache

import (
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"testing/iotest"
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
	if got.Mode != TopologyModeSharded || got.Nodes[0].ID != "node-a" || got.Shards[0].ID != 0 {
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
	if route.Mode != TopologyModeSharded || route.Bucket != nil || !reflect.DeepEqual(route.Owners, []string{wantShard.Primary, wantShard.Replicas[0]}) {
		t.Fatalf("route metadata = %#v, want sharded owners without vbucket", route)
	}

	got.Nodes[0].ID = "mutated"
	if store.Get().Nodes[0].ID != "node-a" {
		t.Fatal("TopologyStore.Get exposed internal node slice")
	}
}

func TestTopologyStoreRoutesVirtualBucketRanges(t *testing.T) {
	topology := ClusterTopology{
		Version:     1,
		Mode:        TopologyModeSharded,
		BucketCount: 16,
		Nodes: []TopologyNode{
			{ID: "node-a"},
			{ID: "node-b"},
		},
		Shards: []TopologyShard{
			{ID: 1, Primary: "node-b"},
			{ID: 0, Primary: "node-a"},
		},
		BucketRanges: []TopologyBucketRange{
			{Start: 8, End: 15, Shard: 1},
			{Start: 0, End: 7, Shard: 0},
		},
	}
	store, err := NewTopologyStore(topology)
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}

	got := store.Get()
	if !reflect.DeepEqual(got.BucketRanges, []TopologyBucketRange{{Start: 0, End: 7, Shard: 0}, {Start: 8, End: 15, Shard: 1}}) {
		t.Fatalf("bucket ranges = %#v, want sorted compact ranges", got.BucketRanges)
	}

	firstRangeKey := topologyKeyForBucketRange(t, store, 0, 7)
	firstRoute, ok := store.Route(firstRangeKey)
	if !ok || firstRoute.Bucket == nil || *firstRoute.Bucket > 7 || firstRoute.Shard.ID != 0 || firstRoute.Shard.Primary != "node-a" {
		t.Fatalf("first range route = %#v/%v, want shard 0 bucket 0..7", firstRoute, ok)
	}

	secondRangeKey := topologyKeyForBucketRange(t, store, 8, 15)
	secondRoute, ok := store.Route(secondRangeKey)
	if !ok || secondRoute.Bucket == nil || *secondRoute.Bucket < 8 || secondRoute.Shard.ID != 1 || secondRoute.Shard.Primary != "node-b" {
		t.Fatalf("second range route = %#v/%v, want shard 1 bucket 8..15", secondRoute, ok)
	}
}

func TestTopologyStoreRoutesFullReplicaMode(t *testing.T) {
	topology := ClusterTopology{
		Version: 1,
		Mode:    TopologyModeFullReplica,
		Self:    "node-b",
		Nodes: []TopologyNode{
			{ID: "node-b"},
			{ID: "node-a"},
		},
	}
	store, err := NewTopologyStore(topology)
	if err != nil {
		t.Fatalf("NewTopologyStore(full replica) error = %v", err)
	}

	route, ok := store.Route("session:1")
	if !ok {
		t.Fatal("Route(full replica) = false, want true")
	}
	if route.Mode != TopologyModeFullReplica || route.Bucket != nil {
		t.Fatalf("route metadata = %#v, want full replica without bucket", route)
	}
	if route.Shard.ID != 0 || route.Shard.Primary != "node-b" || !reflect.DeepEqual(route.Shard.Replicas, []string{"node-a"}) {
		t.Fatalf("route shard = %#v, want self primary and remaining replica", route.Shard)
	}
	if !reflect.DeepEqual(route.Owners, []string{"node-b", "node-a"}) {
		t.Fatalf("route owners = %#v, want primary first", route.Owners)
	}
}

func TestTopologyStoreRejectsInvalidTopology(t *testing.T) {
	for name, topology := range map[string]ClusterTopology{
		"no nodes":        {Version: 1, Shards: []TopologyShard{{ID: 0, Primary: "node-a"}}},
		"no shards":       {Version: 1, Nodes: []TopologyNode{{ID: "node-a"}}},
		"missing primary": {Version: 1, Nodes: []TopologyNode{{ID: "node-a"}}, Shards: []TopologyShard{{ID: 0, Primary: "missing"}}},
		"bad mode":        {Version: 1, Mode: "bad", Nodes: []TopologyNode{{ID: "node-a"}}, Shards: []TopologyShard{{ID: 0, Primary: "node-a"}}},
		"bucket range gap": {
			Version:     1,
			BucketCount: 4,
			Nodes:       []TopologyNode{{ID: "node-a"}},
			Shards:      []TopologyShard{{ID: 0, Primary: "node-a"}},
			BucketRanges: []TopologyBucketRange{
				{Start: 0, End: 1, Shard: 0},
				{Start: 3, End: 3, Shard: 0},
			},
		},
		"bucket range unknown shard": {
			Version:     1,
			BucketCount: 2,
			Nodes:       []TopologyNode{{ID: "node-a"}},
			Shards:      []TopologyShard{{ID: 0, Primary: "node-a"}},
			BucketRanges: []TopologyBucketRange{
				{Start: 0, End: 1, Shard: 1},
			},
		},
		"full replica bucket ranges": {
			Version:      1,
			Mode:         TopologyModeFullReplica,
			Nodes:        []TopologyNode{{ID: "node-a"}},
			BucketRanges: []TopologyBucketRange{{Start: 0, End: 0, Shard: 0}},
		},
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

func topologyKeyForBucketRange(t *testing.T, store *TopologyStore, start uint32, end uint32) string {
	t.Helper()

	for idx := 0; idx < 10000; idx++ {
		key := "key:" + strconv.Itoa(idx)
		route, ok := store.Route(key)
		if !ok || route.Bucket == nil {
			continue
		}
		if *route.Bucket >= start && *route.Bucket <= end {
			return key
		}
	}
	t.Fatalf("no key routed to bucket range %d..%d", start, end)
	return ""
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

func TestDecodeTopologyJSONReaderStreamsTopology(t *testing.T) {
	payload := `{"version":1,"self":"node-a","nodes":[{"id":"node-a","address":"127.0.0.1:8080"}],"shards":[{"id":0,"primary":"node-a"}]}`
	topology, err := decodeTopologyJSONReader(iotest.OneByteReader(strings.NewReader(payload)))
	if err != nil {
		t.Fatalf("decodeTopologyJSONReader() error = %v", err)
	}
	if topology.Version != clusterTopologyVersion || topology.Self != "node-a" || len(topology.Nodes) != 1 || len(topology.Shards) != 1 {
		t.Fatalf("decoded topology = %#v, want one-node topology", topology)
	}
}

func TestDecodeTopologyJSONReaderRejectsInvalidEnvelope(t *testing.T) {
	for name, payload := range map[string]string{
		"unknown":  `{"version":1,"nodes":[{"id":"node-a"}],"shards":[{"id":0,"primary":"node-a"}],"extra":true}`,
		"trailing": `{"version":1,"nodes":[{"id":"node-a"}],"shards":[{"id":0,"primary":"node-a"}]} trailing`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeTopologyJSONReader(strings.NewReader(payload)); err == nil {
				t.Fatal("decodeTopologyJSONReader() error = nil, want rejection")
			}
		})
	}
}
