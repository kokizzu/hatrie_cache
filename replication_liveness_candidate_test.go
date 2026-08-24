package hatriecache

import (
	"reflect"
	"strconv"
	"testing"
	"time"
)

type replicationRoutingLivenessState string

const (
	replicationRoutingLivenessUntrackedHealthy replicationRoutingLivenessState = "UntrackedHealthy"
	replicationRoutingLivenessTrackedHealthy   replicationRoutingLivenessState = "TrackedHealthy"
	replicationRoutingLivenessOffline          replicationRoutingLivenessState = "OneOffline"
	replicationRoutingLivenessTimeout          replicationRoutingLivenessState = "OneTimeout"
	replicationRoutingLivenessMaintenance      replicationRoutingLivenessState = "OneMaintenance"
)

var replicationRoutingLivenessStates = []replicationRoutingLivenessState{
	replicationRoutingLivenessUntrackedHealthy,
	replicationRoutingLivenessTrackedHealthy,
	replicationRoutingLivenessOffline,
	replicationRoutingLivenessTimeout,
	replicationRoutingLivenessMaintenance,
}

func newReplicationRoutingSnapshotOnlineControl(self string, topologyStore *TopologyStore, election *ElectionStore) (replicationRoutingSnapshot, bool) {
	if topologyStore == nil {
		return replicationRoutingSnapshot{}, false
	}
	topology, fingerprint := topologyStore.replicationSnapshot()
	snapshot := replicationRoutingSnapshot{
		topology:    topology,
		self:        self,
		fingerprint: fingerprint,
	}
	if election != nil {
		snapshot.inactive = election.activeNodesSnapshot(topology)
	}
	if topologyMode(topology.Mode) == TopologyModeFullReplica {
		shard, ok := fullReplicaShard(topology)
		if !ok {
			return replicationRoutingSnapshot{}, false
		}
		snapshot.shards = []TopologyShard{shard}
	} else {
		snapshot.shards = topology.Shards
	}
	if len(snapshot.shards) == 0 {
		return replicationRoutingSnapshot{}, false
	}
	snapshot.leaders = make([]ElectionLeader, len(snapshot.shards))
	snapshot.targets = make([][]TopologyNode, len(snapshot.shards))
	for index, shard := range snapshot.shards {
		candidates := routeOwners(shard)
		snapshot.targets[index] = precomputedNormalizedReplicationTargetsOnlineControl(candidates, topology.Nodes, snapshot.inactive, snapshot.self)
		leader := ElectionLeader{
			Shard:      shard.ID,
			Primary:    shard.Primary,
			Candidates: candidates,
		}
		if election == nil {
			leader.Leader = shard.Primary
			leader.Available = true
		} else {
			for _, nodeID := range candidates {
				if snapshot.inactive[nodeID] {
					leader.Leader = nodeID
					leader.Available = true
					break
				}
			}
		}
		snapshot.leaders[index] = leader
	}
	return snapshot, true
}

func replicationRouteTargetsNodeOnlineControl(routing replicationRoutingSnapshot, route ElectionKeyRoute, source string, targetNode string) bool {
	if targetNode == "" || targetNode == source || routing.inactive != nil && !routing.inactive[targetNode] {
		return false
	}
	owners := route.Route.Owners
	if len(owners) == 0 {
		owners = routing.replicationOwners(route.Route.Shard.ID)
	}
	for _, owner := range owners {
		if owner == targetNode {
			return true
		}
	}
	return false
}

func replicationRoutingLivenessFixture(tb testing.TB, size int, state replicationRoutingLivenessState) (*TopologyStore, *ElectionStore, string) {
	tb.Helper()
	topology := replicationRoutingBenchmarkTopology(size)
	lastNode := topology.Nodes[len(topology.Nodes)-1].ID
	if state == replicationRoutingLivenessMaintenance {
		topology.Nodes[len(topology.Nodes)-1].Maintenance = true
		topology.Nodes[len(topology.Nodes)-1].MaintenanceReason = "benchmark"
	}
	store, err := NewTopologyStore(topology)
	if err != nil {
		tb.Fatalf("NewTopologyStore(%d, %s) error = %v", size, state, err)
	}
	now := time.Unix(1_700_000_000, 0)
	election := NewElectionStore(store, ElectionOptions{
		Timeout: time.Second,
		Now:     func() time.Time { return now },
	})
	switch state {
	case replicationRoutingLivenessTrackedHealthy:
		for _, node := range topology.Nodes {
			if err := election.Heartbeat(node.ID); err != nil {
				tb.Fatalf("Heartbeat(%q) error = %v", node.ID, err)
			}
		}
	case replicationRoutingLivenessOffline:
		if err := election.MarkOffline(lastNode); err != nil {
			tb.Fatalf("MarkOffline(%q) error = %v", lastNode, err)
		}
	case replicationRoutingLivenessTimeout:
		if err := election.Heartbeat(lastNode); err != nil {
			tb.Fatalf("Heartbeat(%q) error = %v", lastNode, err)
		}
		now = now.Add(2 * time.Second)
	}
	return store, election, lastNode
}

func replicationRoutingLivenessKeyForShard(tb testing.TB, store *TopologyStore, shardID uint32) string {
	tb.Helper()
	for index := 0; index < 1_000_000; index++ {
		key := "session:" + strconv.Itoa(index)
		route, ok := store.Route(key)
		if ok && route.Shard.ID == shardID {
			return key
		}
	}
	tb.Fatalf("no key routes to shard %d", shardID)
	return ""
}

func TestReplicationRoutingSnapshotSparseInactiveMatchesOnlineControl(t *testing.T) {
	for _, state := range replicationRoutingLivenessStates {
		for _, size := range []int{2, 4, 16, 64} {
			store, election, _ := replicationRoutingLivenessFixture(t, size, state)
			baseline, baselineOK := newReplicationRoutingSnapshotOnlineControl("node-000", store, election)
			candidate, candidateOK := newReplicationRoutingSnapshot("node-000", store, election)
			if candidateOK != baselineOK || !candidateOK {
				t.Fatalf("%s %d-shard snapshot ok = %v/%v", state, size, candidateOK, baselineOK)
			}
			if !reflect.DeepEqual(candidate.topology, baseline.topology) || !reflect.DeepEqual(candidate.shards, baseline.shards) || !reflect.DeepEqual(candidate.leaders, baseline.leaders) || !reflect.DeepEqual(candidate.targets, baseline.targets) || candidate.self != baseline.self || candidate.fingerprint != baseline.fingerprint {
				t.Fatalf("%s %d-shard candidate state = %#v, baseline %#v", state, size, candidate, baseline)
			}
			wantInactive := 0
			if state == replicationRoutingLivenessOffline || state == replicationRoutingLivenessTimeout || state == replicationRoutingLivenessMaintenance {
				wantInactive = 1
			}
			if len(candidate.inactive) != wantInactive {
				t.Fatalf("%s %d-shard candidate inactive nodes = %v, want %d", state, size, candidate.inactive, wantInactive)
			}
			targetIDs := make([]string, 0, len(candidate.topology.Nodes)+1)
			for _, node := range candidate.topology.Nodes {
				baselineActive := baseline.inactive == nil || baseline.inactive[node.ID]
				candidateActive := candidate.inactive == nil || !candidate.inactive[node.ID]
				if candidateActive != baselineActive {
					t.Fatalf("%s %d-shard node %q active = %v, want %v", state, size, node.ID, candidateActive, baselineActive)
				}
				targetIDs = append(targetIDs, node.ID)
			}
			targetIDs = append(targetIDs, "", "missing-node")
			for keyIndex := 0; keyIndex < 4096; keyIndex++ {
				key := "session:" + strconv.Itoa(keyIndex)
				want, wantTargets, wantOK := baseline.routeForKeyAndTargets(key)
				got, gotTargets, gotOK := candidate.routeForKeyAndTargets(key)
				if gotOK != wantOK || !reflect.DeepEqual(got, want) || !reflect.DeepEqual(gotTargets, wantTargets) {
					t.Fatalf("%s %d-shard route(%q) = %#v/%#v/%v, want %#v/%#v/%v", state, size, key, got, gotTargets, gotOK, want, wantTargets, wantOK)
				}
				if keyIndex%257 != 0 {
					continue
				}
				for _, source := range []string{"", candidate.self, got.Leader.Leader} {
					for _, targetID := range targetIDs {
						wantMembership := replicationRouteTargetsNodeOnlineControl(baseline, want, source, targetID)
						gotMembership := replicationRouteTargetsNode(candidate, got, source, targetID)
						if gotMembership != wantMembership {
							t.Fatalf("%s %d-shard route(%q) source=%q target=%q membership = %v, want %v", state, size, key, source, targetID, gotMembership, wantMembership)
						}
					}
				}
			}
		}
	}
}

func BenchmarkReplicationRoutingInactiveNodesConstructionAlternating(b *testing.B) {
	for _, state := range replicationRoutingLivenessStates {
		b.Run(string(state), func(b *testing.B) {
			for _, size := range []int{2, 4, 8, 16, 32, 64} {
				b.Run(strconv.Itoa(size)+"Shards", func(b *testing.B) {
					store, election, _ := replicationRoutingLivenessFixture(b, size, state)
					var baselineDuration, candidateDuration time.Duration
					b.ResetTimer()
					for iteration := 0; iteration < b.N; iteration++ {
						candidateFirst := iteration&1 != 0
						for pass := 0; pass < 2; pass++ {
							started := time.Now()
							var snapshot replicationRoutingSnapshot
							var ok bool
							if candidateFirst == (pass == 0) {
								snapshot, ok = newReplicationRoutingSnapshot("node-000", store, election)
								candidateDuration += time.Since(started)
							} else {
								snapshot, ok = newReplicationRoutingSnapshotOnlineControl("node-000", store, election)
								baselineDuration += time.Since(started)
							}
							if !ok {
								b.Fatal("routing snapshot construction failed")
							}
							benchmarkReplicationRoutingSnapshotSink = snapshot
						}
					}
					b.StopTimer()
					b.ReportMetric(float64(baselineDuration.Nanoseconds())/float64(b.N), "baseline_ns/snapshot")
					b.ReportMetric(float64(candidateDuration.Nanoseconds())/float64(b.N), "candidate_ns/snapshot")
				})
			}
		})
	}
}

func BenchmarkReplicationRoutingInactiveNodesConstruction(b *testing.B) {
	for _, state := range replicationRoutingLivenessStates {
		for _, size := range []int{2, 4, 8, 16, 32, 64} {
			store, election, _ := replicationRoutingLivenessFixture(b, size, state)
			for _, candidate := range []bool{false, true} {
				layout := "Baseline"
				if candidate {
					layout = "Candidate"
				}
				b.Run(string(state)+"/"+strconv.Itoa(size)+"Shards/"+layout, func(b *testing.B) {
					b.ReportAllocs()
					for iteration := 0; iteration < b.N; iteration++ {
						var snapshot replicationRoutingSnapshot
						var ok bool
						if candidate {
							snapshot, ok = newReplicationRoutingSnapshot("node-000", store, election)
						} else {
							snapshot, ok = newReplicationRoutingSnapshotOnlineControl("node-000", store, election)
						}
						if !ok {
							b.Fatal("routing snapshot construction failed")
						}
						benchmarkReplicationRoutingSnapshotSink = snapshot
					}
				})
			}
		}
	}
}

func BenchmarkReplicationRoutingInactiveNodeMembershipAlternating(b *testing.B) {
	for _, state := range []replicationRoutingLivenessState{replicationRoutingLivenessUntrackedHealthy, replicationRoutingLivenessOffline} {
		b.Run(string(state), func(b *testing.B) {
			store, election, targetID := replicationRoutingLivenessFixture(b, 64, state)
			baseline, ok := newReplicationRoutingSnapshotOnlineControl("node-000", store, election)
			if !ok {
				b.Fatal("baseline routing snapshot construction failed")
			}
			candidate, ok := newReplicationRoutingSnapshot("node-000", store, election)
			if !ok {
				b.Fatal("candidate routing snapshot construction failed")
			}
			key := replicationRoutingLivenessKeyForShard(b, store, 63)
			baselineRoute, ok := baseline.routeForKey(key)
			if !ok {
				b.Fatal("baseline route failed")
			}
			candidateRoute, ok := candidate.routeForKey(key)
			if !ok {
				b.Fatal("candidate route failed")
			}
			var baselineDuration, candidateDuration time.Duration
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				candidateFirst := iteration&1 != 0
				for pass := 0; pass < 2; pass++ {
					started := time.Now()
					if candidateFirst == (pass == 0) {
						benchmarkReplicationRouteTargetSink = replicationRouteTargetsNode(candidate, candidateRoute, "node-000", targetID)
						candidateDuration += time.Since(started)
					} else {
						benchmarkReplicationRouteTargetSink = replicationRouteTargetsNodeOnlineControl(baseline, baselineRoute, "node-000", targetID)
						baselineDuration += time.Since(started)
					}
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(baselineDuration.Nanoseconds())/float64(b.N), "baseline_ns/check")
			b.ReportMetric(float64(candidateDuration.Nanoseconds())/float64(b.N), "candidate_ns/check")
		})
	}
}
