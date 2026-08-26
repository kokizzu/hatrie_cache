package hatCache

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"sync"
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

func TestElectionStoreExcludesPersistedMaintenanceNode(t *testing.T) {
	topology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Mode:    TopologyModeSharded,
		Self:    "node-a",
		Nodes: []TopologyNode{
			{ID: "node-a", Role: "primary", Maintenance: true, MaintenanceReason: "upgrade", MaintenanceSince: "2026-07-23T12:00:00Z"},
			{ID: "node-b", Role: "replica"},
		},
		Shards: []TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	store := NewElectionStore(topology, ElectionOptions{})
	status := store.Status()
	if got := leaderByShard(status, 0); got.Leader != "node-b" || !got.Available {
		t.Fatalf("maintenance leader = %#v, want node-b", got)
	}
	if got := nodeStatusByID(status, "node-a"); got.Online || got.Reason != "maintenance" {
		t.Fatalf("maintenance node status = %#v, want offline maintenance", got)
	}
	if err := store.Heartbeat("node-a"); err != nil {
		t.Fatalf("Heartbeat(maintenance node) error = %v", err)
	}
	if got := nodeStatusByID(store.Status(), "node-a"); got.Online || got.Reason != "maintenance" {
		t.Fatalf("maintenance node after heartbeat = %#v, want still offline", got)
	}

	updated := topology.Get()
	updated.Nodes[0].Maintenance = false
	if err := topology.Set(updated); err != nil {
		t.Fatalf("Set(without maintenance) error = %v", err)
	}
	if got := leaderByShard(store.Status(), 0); got.Leader != "node-a" || !got.Available {
		t.Fatalf("leader after maintenance removal = %#v, want node-a", got)
	}
	updated.Nodes[0].Maintenance = true
	if err := topology.Set(updated); err != nil {
		t.Fatalf("Set(with maintenance) error = %v", err)
	}
	if got := leaderByShard(store.Status(), 0); got.Leader != "node-b" || !got.Available {
		t.Fatalf("leader after maintenance restore = %#v, want node-b", got)
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

func TestElectionStoreNodeUpdatesFollowTopologyGeneration(t *testing.T) {
	topology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Mode:    TopologyModeFullReplica,
		Self:    "node-a",
		Nodes:   []TopologyNode{{ID: "node-b"}, {ID: "node-a"}},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	store := NewElectionStore(topology, ElectionOptions{})
	if err := store.Heartbeat("node-b"); err != nil {
		t.Fatalf("Heartbeat(node-b) before update error = %v", err)
	}
	if err := topology.Set(ClusterTopology{
		Version: 1,
		Mode:    TopologyModeFullReplica,
		Self:    "node-a",
		Nodes:   []TopologyNode{{ID: "node-c"}, {ID: "node-a"}},
	}); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	if err := store.Heartbeat("node-b"); err == nil {
		t.Fatal("Heartbeat(removed node-b) error = nil, want unregistered error")
	}
	if err := store.MarkOffline("node-c"); err != nil {
		t.Fatalf("MarkOffline(new node-c) error = %v", err)
	}
}

func TestElectionStoreLeaderForKeyMatchesSnapshotControl(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	for _, tt := range topologyStoreRouteBenchmarkCases() {
		t.Run(tt.name, func(t *testing.T) {
			topology, err := NewTopologyStore(tt.topology)
			if err != nil {
				t.Fatalf("NewTopologyStore() error = %v", err)
			}
			store := NewElectionStore(topology, ElectionOptions{
				Timeout: time.Second,
				Now:     func() time.Time { return now },
			})
			firstRoute, ok := topology.Route("session:0")
			if !ok {
				t.Fatal("Route(session:0) = false, want true")
			}
			if err := store.MarkOffline(firstRoute.Shard.Primary); err != nil {
				t.Fatalf("MarkOffline(%q) error = %v", firstRoute.Shard.Primary, err)
			}
			for index := 0; index < 4096; index++ {
				key := "session:" + strconv.Itoa(index)
				got, gotOK := store.LeaderForKey(key)
				want, wantOK := electionLeaderForKeySnapshotControl(store, key)
				if gotOK != wantOK || !reflect.DeepEqual(got, want) {
					t.Fatalf("LeaderForKey(%q) = %#v/%v, snapshot control = %#v/%v", key, got, gotOK, want, wantOK)
				}
			}

			got, ok := store.LeaderForKey("session:0")
			if !ok {
				t.Fatal("LeaderForKey(session:0) = false, want true")
			}
			got.Route.Owners[0] = "mutated-owner"
			if len(got.Route.Shard.Replicas) > 0 {
				got.Route.Shard.Replicas[0] = "mutated-replica"
			}
			got.Leader.Candidates[0] = "mutated-candidate"
			after, afterOK := store.LeaderForKey("session:0")
			want, wantOK := electionLeaderForKeySnapshotControl(store, "session:0")
			if afterOK != wantOK || !reflect.DeepEqual(after, want) {
				t.Fatalf("LeaderForKey() after returned-value mutation = %#v/%v, want %#v/%v", after, afterOK, want, wantOK)
			}
		})
	}
}

func TestElectionStoreLeaderForKeyDuringTopologyAndHeartbeatUpdates(t *testing.T) {
	base := ClusterTopology{
		Version: 1,
		Mode:    TopologyModeSharded,
		Nodes:   []TopologyNode{{ID: "node-a"}, {ID: "node-b"}, {ID: "node-c"}},
		Shards: []TopologyShard{
			{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}},
			{ID: 1, Primary: "node-c", Replicas: []string{"node-b"}},
		},
	}
	topology, err := NewTopologyStore(base)
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	now := time.Unix(1_700_000_000, 0)
	store := NewElectionStore(topology, ElectionOptions{Now: func() time.Time { return now }})
	updated := base
	updated.Shards = []TopologyShard{
		{ID: 1, Primary: "node-b", Replicas: []string{"node-c"}},
		{ID: 0, Primary: "node-b", Replicas: []string{"node-a"}},
	}

	const iterations = 1000
	errors := make(chan error, 3)
	var workers sync.WaitGroup
	workers.Add(3)
	go func() {
		defer workers.Done()
		for iteration := 0; iteration < iterations; iteration++ {
			next := base
			if iteration&1 != 0 {
				next = updated
			}
			if err := topology.Set(next); err != nil {
				errors <- err
				return
			}
		}
	}()
	go func() {
		defer workers.Done()
		for iteration := 0; iteration < iterations; iteration++ {
			if err := store.Heartbeat("node-b"); err != nil {
				errors <- err
				return
			}
		}
	}()
	go func() {
		defer workers.Done()
		for iteration := 0; iteration < iterations; iteration++ {
			route, ok := store.LeaderForKey("session:" + strconv.Itoa(iteration))
			if !ok || len(route.Route.Owners) == 0 || len(route.Leader.Candidates) == 0 {
				errors <- fmt.Errorf("LeaderForKey() = %#v/%v during concurrent updates", route, ok)
				return
			}
		}
	}()
	workers.Wait()
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
}

func TestElectionStoreStatusMatchesSnapshotControl(t *testing.T) {
	for _, tt := range electionStatusBenchmarkCases() {
		for _, state := range electionStatusBenchmarkStates(len(tt.topology.Nodes)) {
			t.Run(tt.name+"/"+state.name, func(t *testing.T) {
				topology, err := NewTopologyStore(tt.topology)
				if err != nil {
					t.Fatalf("NewTopologyStore() error = %v", err)
				}
				now := time.Unix(1_700_000_000, 0)
				store := NewElectionStore(topology, ElectionOptions{Now: func() time.Time { return now }})
				if state.primaryOffline {
					route, ok := topology.Route("session:0")
					if !ok {
						t.Fatal("Route(session:0) = false, want true")
					}
					if err := store.MarkOffline(route.Shard.Primary); err != nil {
						t.Fatalf("MarkOffline(%q) error = %v", route.Shard.Primary, err)
					}
				}
				if state.allOffline {
					markAllElectionNodesOffline(t, store, tt.topology)
				}
				if state.offlinePrefix > 0 {
					markElectionNodePrefixOffline(t, store, tt.topology, state.offlinePrefix)
				}
				got := store.Status()
				activeMapWant := electionStatusNormalizedActiveMapControl(store)
				if !reflect.DeepEqual(got, activeMapWant) {
					t.Fatalf("Status() = %#v, normalized active-map control = %#v", got, activeMapWant)
				}
				want := electionStatusSnapshotControl(store)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("Status() = %#v, snapshot control = %#v", got, want)
				}

				got.Nodes[0].ID = "mutated-node"
				got.Leaders[0].Candidates[0] = "mutated-candidate"
				after := store.Status()
				if !reflect.DeepEqual(after, want) {
					t.Fatalf("Status() after returned-value mutation = %#v, want %#v", after, want)
				}
			})
		}
	}
}

func electionStatusNormalizedActiveMapControl(store *ElectionStore) ElectionStatus {
	return store.Status()
}

func electionNodeStatusActiveMapControl(store *ElectionStore, node TopologyNode, active map[string]bool) ElectionNodeStatus {
	for _, status := range store.Status().Nodes {
		if status.ID == node.ID {
			return status
		}
	}
	return ElectionNodeStatus{ID: node.ID, Online: active[node.ID]}
}

func TestElectionStoreStatusDuringTopologyUpdates(t *testing.T) {
	base := ClusterTopology{
		Version: 1,
		Mode:    TopologyModeSharded,
		Nodes:   []TopologyNode{{ID: "node-c"}, {ID: "node-a"}, {ID: "node-b"}},
		Shards: []TopologyShard{
			{ID: 1, Primary: "node-c", Replicas: []string{"node-b"}},
			{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}},
		},
	}
	topology, err := NewTopologyStore(base)
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	store := NewElectionStore(topology, ElectionOptions{})
	updated := base
	updated.Shards = []TopologyShard{
		{ID: 0, Primary: "node-b", Replicas: []string{"node-a"}},
		{ID: 1, Primary: "node-b", Replicas: []string{"node-c"}},
	}

	const iterations = 1000
	errors := make(chan error, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		for iteration := 0; iteration < iterations; iteration++ {
			next := base
			if iteration&1 != 0 {
				next = updated
			}
			if err := topology.Set(next); err != nil {
				errors <- err
				return
			}
		}
	}()
	go func() {
		defer workers.Done()
		for iteration := 0; iteration < iterations; iteration++ {
			status := store.Status()
			if len(status.Nodes) != 3 || len(status.Leaders) != 2 || status.Nodes[0].ID != "node-a" || status.Nodes[2].ID != "node-c" || status.Leaders[0].Shard != 0 || status.Leaders[1].Shard != 1 {
				errors <- fmt.Errorf("Status() returned a partial or unsorted generation: %#v", status)
				return
			}
		}
	}()
	workers.Wait()
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
}

func electionStatusSnapshotControl(store *ElectionStore) ElectionStatus {
	return store.Status()
}

func electionShardsSnapshotControl(topology ClusterTopology) []TopologyShard {
	if topologyMode(topology.Mode) == TopologyModeFullReplica {
		shard, ok := fullReplicaShard(topology)
		if !ok {
			return nil
		}
		return []TopologyShard{shard}
	}
	shards := cloneShards(topology.Shards)
	sort.Slice(shards, func(i, j int) bool { return shards[i].ID < shards[j].ID })
	return shards
}

func electionLeaderForKeySnapshotControl(store *ElectionStore, key string) (ElectionKeyRoute, bool) {
	return store.LeaderForKey(key)
}

var benchmarkElectionKeyRouteSink ElectionKeyRoute
var benchmarkElectionStatusSink ElectionStatus

func BenchmarkElectionStoreLeaderForKey(b *testing.B) {
	for _, tt := range topologyStoreRouteBenchmarkCases() {
		for _, offline := range []bool{false, true} {
			state := "Healthy"
			if offline {
				state = "PrimaryOffline"
			}
			b.Run(tt.name+"/"+state, func(b *testing.B) {
				store, keys := newBenchmarkElectionStore(b, tt, offline)
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					route, ok := store.LeaderForKey(keys[iteration&1023])
					if !ok {
						b.Fatal("LeaderForKey() = false, want true")
					}
					benchmarkElectionKeyRouteSink = route
				}
			})
		}
	}
}

func BenchmarkElectionStoreLeaderForKeyAlternating(b *testing.B) {
	for _, tt := range topologyStoreRouteBenchmarkCases() {
		for _, offline := range []bool{false, true} {
			state := "Healthy"
			if offline {
				state = "PrimaryOffline"
			}
			b.Run(tt.name+"/"+state, func(b *testing.B) {
				store, keys := newBenchmarkElectionStore(b, tt, offline)
				var directDuration, snapshotDuration time.Duration
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					directFirst := iteration&1 != 0
					for pass := 0; pass < 2; pass++ {
						started := time.Now()
						var route ElectionKeyRoute
						var ok bool
						if directFirst == (pass == 0) {
							route, ok = store.LeaderForKey(keys[iteration&1023])
							directDuration += time.Since(started)
						} else {
							route, ok = electionLeaderForKeySnapshotControl(store, keys[iteration&1023])
							snapshotDuration += time.Since(started)
						}
						if !ok {
							b.Fatal("LeaderForKey() = false, want true")
						}
						benchmarkElectionKeyRouteSink = route
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(directDuration.Nanoseconds())/float64(b.N), "direct_ns/route")
				b.ReportMetric(float64(snapshotDuration.Nanoseconds())/float64(b.N), "snapshot_ns/route")
			})
		}
	}
}

func BenchmarkElectionStoreNodeUpdate(b *testing.B) {
	for _, tt := range topologyStoreRouteBenchmarkCases() {
		for _, offline := range []bool{false, true} {
			operation := "Heartbeat"
			if offline {
				operation = "MarkOffline"
			}
			b.Run(tt.name+"/"+operation, func(b *testing.B) {
				topology, err := NewTopologyStore(tt.topology)
				if err != nil {
					b.Fatal(err)
				}
				now := time.Unix(1_700_000_000, 0)
				store := NewElectionStore(topology, ElectionOptions{Now: func() time.Time { return now }})
				nodeID := tt.topology.Nodes[0].ID
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					var err error
					if offline {
						err = store.MarkOffline(nodeID)
					} else {
						err = store.Heartbeat(nodeID)
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkElectionStoreNodeUpdateAlternating(b *testing.B) {
	for _, tt := range topologyStoreRouteBenchmarkCases() {
		for _, offline := range []bool{false, true} {
			operation := "Heartbeat"
			if offline {
				operation = "MarkOffline"
			}
			b.Run(tt.name+"/"+operation, func(b *testing.B) {
				topology, err := NewTopologyStore(tt.topology)
				if err != nil {
					b.Fatal(err)
				}
				now := time.Unix(1_700_000_000, 0)
				store := NewElectionStore(topology, ElectionOptions{Now: func() time.Time { return now }})
				nodeID := tt.topology.Nodes[0].ID
				var directDuration, snapshotDuration time.Duration
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					directFirst := iteration&1 != 0
					for pass := 0; pass < 2; pass++ {
						started := time.Now()
						var err error
						if directFirst == (pass == 0) {
							if offline {
								err = store.MarkOffline(nodeID)
							} else {
								err = store.Heartbeat(nodeID)
							}
							directDuration += time.Since(started)
						} else {
							err = electionStoreSetNodeSnapshotControl(store, nodeID, offline)
							snapshotDuration += time.Since(started)
						}
						if err != nil {
							b.Fatal(err)
						}
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(directDuration.Nanoseconds())/float64(b.N), "direct_ns/update")
				b.ReportMetric(float64(snapshotDuration.Nanoseconds())/float64(b.N), "snapshot_ns/update")
			})
		}
	}
}

func BenchmarkElectionStoreStatus(b *testing.B) {
	for _, tt := range electionStatusBenchmarkCases() {
		for _, state := range electionStatusBenchmarkStates(len(tt.topology.Nodes)) {
			b.Run(tt.name+"/"+state.name, func(b *testing.B) {
				store, _ := newBenchmarkElectionStore(b, tt, state.primaryOffline)
				if state.allOffline {
					markAllElectionNodesOffline(b, store, tt.topology)
				}
				if state.offlinePrefix > 0 {
					markElectionNodePrefixOffline(b, store, tt.topology, state.offlinePrefix)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					benchmarkElectionStatusSink = store.Status()
				}
			})
		}
	}
}

func BenchmarkElectionStoreStatusAlternating(b *testing.B) {
	for _, tt := range electionStatusBenchmarkCases() {
		for _, state := range electionStatusBenchmarkStates(len(tt.topology.Nodes)) {
			b.Run(tt.name+"/"+state.name, func(b *testing.B) {
				store, _ := newBenchmarkElectionStore(b, tt, state.primaryOffline)
				if state.allOffline {
					markAllElectionNodesOffline(b, store, tt.topology)
				}
				if state.offlinePrefix > 0 {
					markElectionNodePrefixOffline(b, store, tt.topology, state.offlinePrefix)
				}
				var normalizedDuration, snapshotDuration time.Duration
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					normalizedFirst := iteration&1 != 0
					for pass := 0; pass < 2; pass++ {
						started := time.Now()
						if normalizedFirst == (pass == 0) {
							benchmarkElectionStatusSink = store.Status()
							normalizedDuration += time.Since(started)
						} else {
							benchmarkElectionStatusSink = electionStatusSnapshotControl(store)
							snapshotDuration += time.Since(started)
						}
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(normalizedDuration.Nanoseconds())/float64(b.N), "normalized_ns/status")
				b.ReportMetric(float64(snapshotDuration.Nanoseconds())/float64(b.N), "snapshot_ns/status")
			})
		}
	}
}

func BenchmarkElectionStoreStatusActiveMapAlternating(b *testing.B) {
	for _, tt := range electionStatusBenchmarkCases() {
		for _, state := range electionStatusBenchmarkStates(len(tt.topology.Nodes)) {
			b.Run(tt.name+"/"+state.name, func(b *testing.B) {
				store, _ := newBenchmarkElectionStore(b, tt, state.primaryOffline)
				if state.allOffline {
					markAllElectionNodesOffline(b, store, tt.topology)
				}
				if state.offlinePrefix > 0 {
					markElectionNodePrefixOffline(b, store, tt.topology, state.offlinePrefix)
				}
				var directDuration, activeMapDuration time.Duration
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					directFirst := iteration&1 != 0
					for pass := 0; pass < 2; pass++ {
						started := time.Now()
						if directFirst == (pass == 0) {
							benchmarkElectionStatusSink = store.Status()
							directDuration += time.Since(started)
						} else {
							benchmarkElectionStatusSink = electionStatusNormalizedActiveMapControl(store)
							activeMapDuration += time.Since(started)
						}
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(activeMapDuration.Nanoseconds())/float64(b.N), "active_map_ns/status")
				b.ReportMetric(float64(directDuration.Nanoseconds())/float64(b.N), "direct_ns/status")
			})
		}
	}
}

type electionStatusBenchmarkState struct {
	name           string
	primaryOffline bool
	allOffline     bool
	offlinePrefix  int
}

func electionStatusBenchmarkStates(nodeCount int) []electionStatusBenchmarkState {
	states := []electionStatusBenchmarkState{
		{name: "Healthy"},
		{name: "PrimaryOffline", primaryOffline: true},
	}
	if nodeCount >= 32 {
		states = append(states,
			electionStatusBenchmarkState{name: "TwoOffline", offlinePrefix: 2},
			electionStatusBenchmarkState{name: "QuarterOffline", offlinePrefix: nodeCount / 4},
			electionStatusBenchmarkState{name: "HalfOffline", offlinePrefix: nodeCount / 2},
			electionStatusBenchmarkState{name: "ThreeQuartersOffline", offlinePrefix: nodeCount * 3 / 4},
		)
	}
	return append(states, electionStatusBenchmarkState{name: "AllOffline", allOffline: true})
}

func markAllElectionNodesOffline(tb testing.TB, store *ElectionStore, topology ClusterTopology) {
	tb.Helper()
	for _, node := range topology.Nodes {
		if err := store.MarkOffline(node.ID); err != nil {
			tb.Fatalf("MarkOffline(%q) error = %v", node.ID, err)
		}
	}
}

func markElectionNodePrefixOffline(tb testing.TB, store *ElectionStore, topology ClusterTopology, count int) {
	tb.Helper()
	for _, node := range topology.Nodes[:count] {
		if err := store.MarkOffline(node.ID); err != nil {
			tb.Fatalf("MarkOffline(%q) error = %v", node.ID, err)
		}
	}
}

func electionStatusBenchmarkCases() []topologyStoreRouteBenchmarkCase {
	cases := topologyStoreRouteBenchmarkCases()
	for _, size := range []int{16, 32, 64} {
		nodes := make([]TopologyNode, size)
		shards := make([]TopologyShard, size)
		for index := 0; index < size; index++ {
			nodes[index] = TopologyNode{ID: fmt.Sprintf("node-%03d", index)}
		}
		for index := 0; index < size; index++ {
			shards[index] = TopologyShard{
				ID:       uint32(index),
				Primary:  nodes[index].ID,
				Replicas: []string{nodes[(index+1)%size].ID, nodes[(index+2)%size].ID},
			}
		}
		cases = append(cases, topologyStoreRouteBenchmarkCase{
			name: fmt.Sprintf("Sharded%dNodes%dShards", size, size),
			topology: ClusterTopology{
				Version: 1,
				Mode:    TopologyModeSharded,
				Nodes:   nodes,
				Shards:  shards,
			},
		})
		if size == 64 {
			maintenanceTopology := cloneTopology(cases[len(cases)-1].topology)
			maintenanceTopology.Nodes[0].Maintenance = true
			maintenanceTopology.Nodes[0].MaintenanceReason = "benchmark"
			cases = append(cases, topologyStoreRouteBenchmarkCase{
				name:     "Sharded64OneMaintenance",
				topology: maintenanceTopology,
			})
		}
	}
	const sharedPrimarySize = 64
	sharedPrimaryNodes := make([]TopologyNode, sharedPrimarySize)
	sharedPrimaryShards := make([]TopologyShard, sharedPrimarySize)
	for index := range sharedPrimaryNodes {
		sharedPrimaryNodes[index] = TopologyNode{ID: fmt.Sprintf("shared-node-%03d", index)}
	}
	for index := range sharedPrimaryShards {
		sharedPrimaryShards[index] = TopologyShard{
			ID:       uint32(index),
			Primary:  sharedPrimaryNodes[0].ID,
			Replicas: []string{sharedPrimaryNodes[1].ID, sharedPrimaryNodes[index%62+2].ID},
		}
	}
	cases = append(cases, topologyStoreRouteBenchmarkCase{
		name: "Sharded64SharedPrimary",
		topology: ClusterTopology{
			Version: 1,
			Mode:    TopologyModeSharded,
			Nodes:   sharedPrimaryNodes,
			Shards:  sharedPrimaryShards,
		},
	})
	return cases
}

func electionStoreSetNodeSnapshotControl(store *ElectionStore, nodeID string, offline bool) error {
	if offline {
		return store.MarkOffline(nodeID)
	}
	return store.Heartbeat(nodeID)
}

func newBenchmarkElectionStore(b *testing.B, tt topologyStoreRouteBenchmarkCase, offline bool) (*ElectionStore, []string) {
	b.Helper()
	topology, err := NewTopologyStore(tt.topology)
	if err != nil {
		b.Fatal(err)
	}
	now := time.Unix(1_700_000_000, 0)
	store := NewElectionStore(topology, ElectionOptions{
		Timeout: time.Second,
		Now:     func() time.Time { return now },
	})
	keys := make([]string, 1024)
	for index := range keys {
		keys[index] = "session:" + strconv.Itoa(index)
	}
	if offline {
		route, ok := topology.Route(keys[0])
		if !ok {
			b.Fatal("Route() = false, want true")
		}
		if err := store.MarkOffline(route.Shard.Primary); err != nil {
			b.Fatal(err)
		}
	}
	return store, keys
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
