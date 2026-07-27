package hatriecache

import (
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func precomputedNormalizedReplicationTargetsReflectiveSortControl(owners []string, nodes []TopologyNode, inactive map[string]bool, self string) []TopologyNode {
	targets := make([]TopologyNode, 0, len(owners))
	for _, nodeID := range owners {
		nodeID = strings.TrimSpace(nodeID)
		if nodeID == "" || nodeID == self || inactive[nodeID] {
			continue
		}
		node, ok := normalizedTopologyNode(nodes, nodeID)
		if !ok {
			continue
		}
		targets = append(targets, node)
	}
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].ID < targets[j].ID
	})
	return targets
}

func newReplicationRoutingSnapshotReflectiveTargetSortControl(self string, topologyStore *TopologyStore, election *ElectionStore) (replicationRoutingSnapshot, bool) {
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
		snapshot.inactive = election.inactiveNodesSnapshot(topology)
	}
	if topologyMode(topology.Mode) == TopologyModeFullReplica {
		shard, ok := topology.fullReplicaShard()
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
		snapshot.targets[index] = precomputedNormalizedReplicationTargetsReflectiveSortControl(candidates, topology.Nodes, snapshot.inactive, snapshot.self)
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
				if !snapshot.inactive[nodeID] {
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

func replicationRoutingFullReplicaSortFixture(tb testing.TB, size int) (*TopologyStore, *ElectionStore) {
	tb.Helper()
	topology := replicationRoutingBenchmarkTopology(size)
	topology.Mode = TopologyModeFullReplica
	topology.Shards = nil
	store, err := NewTopologyStore(topology)
	if err != nil {
		tb.Fatalf("NewTopologyStore(full replica %d) error = %v", size, err)
	}
	return store, NewElectionStore(store, ElectionOptions{})
}

func compareReplicationRoutingAdaptiveTargetSort(tb testing.TB, name string, baseline replicationRoutingSnapshot, baselineOK bool, candidate replicationRoutingSnapshot, candidateOK bool) {
	tb.Helper()
	if candidateOK != baselineOK || !candidateOK {
		tb.Fatalf("%s snapshot ok = %v/%v", name, candidateOK, baselineOK)
	}
	if !reflect.DeepEqual(candidate.topology, baseline.topology) || !reflect.DeepEqual(candidate.shards, baseline.shards) || !reflect.DeepEqual(candidate.inactive, baseline.inactive) || !reflect.DeepEqual(candidate.leaders, baseline.leaders) || !reflect.DeepEqual(candidate.targets, baseline.targets) || candidate.self != baseline.self || candidate.fingerprint != baseline.fingerprint {
		tb.Fatalf("%s candidate state = %#v, baseline %#v", name, candidate, baseline)
	}
	for keyIndex := 0; keyIndex < 4096; keyIndex++ {
		key := "session:" + strconv.Itoa(keyIndex)
		want, wantTargets, wantOK := baseline.routeForKeyAndTargets(key)
		got, gotTargets, gotOK := candidate.routeForKeyAndTargets(key)
		if gotOK != wantOK || !reflect.DeepEqual(got, want) || !reflect.DeepEqual(gotTargets, wantTargets) {
			tb.Fatalf("%s route(%q) = %#v/%#v/%v, want %#v/%#v/%v", name, key, got, gotTargets, gotOK, want, wantTargets, wantOK)
		}
	}
}

func TestReplicationRoutingSnapshotAdaptiveTargetSortMatchesReflectiveControl(t *testing.T) {
	for _, state := range replicationRoutingLivenessStates {
		for _, size := range []int{2, 4, 16, 64} {
			store, election, _ := replicationRoutingLivenessFixture(t, size, state)
			baseline, baselineOK := newReplicationRoutingSnapshotReflectiveTargetSortControl("node-000", store, election)
			candidate, candidateOK := newReplicationRoutingSnapshot("node-000", store, election)
			compareReplicationRoutingAdaptiveTargetSort(t, string(state)+"/"+strconv.Itoa(size), baseline, baselineOK, candidate, candidateOK)
		}
	}
	for _, size := range []int{2, 4, 8, 16, 32, 64} {
		store, election := replicationRoutingFullReplicaSortFixture(t, size)
		baseline, baselineOK := newReplicationRoutingSnapshotReflectiveTargetSortControl("node-000", store, election)
		candidate, candidateOK := newReplicationRoutingSnapshot("node-000", store, election)
		compareReplicationRoutingAdaptiveTargetSort(t, "FullReplica/"+strconv.Itoa(size), baseline, baselineOK, candidate, candidateOK)
	}
}

func BenchmarkReplicationRoutingAdaptiveTargetSortConstructionAlternating(b *testing.B) {
	for _, state := range []replicationRoutingLivenessState{replicationRoutingLivenessUntrackedHealthy, replicationRoutingLivenessOffline} {
		b.Run(string(state), func(b *testing.B) {
			for _, size := range []int{2, 4, 8, 16, 32, 64} {
				b.Run(strconv.Itoa(size)+"Shards", func(b *testing.B) {
					store, election, _ := replicationRoutingLivenessFixture(b, size, state)
					benchmarkReplicationRoutingAdaptiveTargetSortAlternating(b, store, election)
				})
			}
		})
	}
	for _, size := range []int{2, 4, 8, 16, 32, 64} {
		b.Run("FullReplica/"+strconv.Itoa(size)+"Nodes", func(b *testing.B) {
			store, election := replicationRoutingFullReplicaSortFixture(b, size)
			benchmarkReplicationRoutingAdaptiveTargetSortAlternating(b, store, election)
		})
	}
}

func benchmarkReplicationRoutingAdaptiveTargetSortAlternating(b *testing.B, store *TopologyStore, election *ElectionStore) {
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
				snapshot, ok = newReplicationRoutingSnapshotReflectiveTargetSortControl("node-000", store, election)
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
}

func BenchmarkReplicationRoutingAdaptiveTargetSortConstruction(b *testing.B) {
	for _, state := range []replicationRoutingLivenessState{replicationRoutingLivenessUntrackedHealthy, replicationRoutingLivenessOffline} {
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
							snapshot, ok = newReplicationRoutingSnapshotReflectiveTargetSortControl("node-000", store, election)
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
	for _, size := range []int{2, 4, 8, 16, 32, 64} {
		store, election := replicationRoutingFullReplicaSortFixture(b, size)
		for _, candidate := range []bool{false, true} {
			layout := "Baseline"
			if candidate {
				layout = "Candidate"
			}
			b.Run("FullReplica/"+strconv.Itoa(size)+"Nodes/"+layout, func(b *testing.B) {
				b.ReportAllocs()
				for iteration := 0; iteration < b.N; iteration++ {
					var snapshot replicationRoutingSnapshot
					var ok bool
					if candidate {
						snapshot, ok = newReplicationRoutingSnapshot("node-000", store, election)
					} else {
						snapshot, ok = newReplicationRoutingSnapshotReflectiveTargetSortControl("node-000", store, election)
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
