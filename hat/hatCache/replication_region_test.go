package hatCache

import (
	"testing"
	"time"
)

func TestReplicationRegionPolicyReportsRemoteRPOStatus(t *testing.T) {
	topology, err := NewTopologyStore(ClusterTopology{
		Version: clusterTopologyVersion,
		Mode:    TopologyModeFullReplica,
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://node-a", Region: "asia"},
			{ID: "node-b", Address: "http://node-b", Region: "europe"},
		},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	policy := ReplicationRegionPolicy{
		LocalRegion:           "asia",
		RequiredRemoteRegions: []string{"europe", "us"},
		MaxRPOLagSequences:    1,
		MaxRTO:                5 * time.Second,
	}
	if err := policy.Validate(); err != nil {
		t.Fatalf("ReplicationRegionPolicy.Validate() error = %v", err)
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:                    "node-a",
		Topology:                topology,
		ReplicationRegionPolicy: policy,
	})
	t.Cleanup(replicator.Close)
	replicator.mu.Lock()
	replicator.queueStats.SourceSequence = 3
	replicator.queueStats.LastAcknowledgedSequenceByTarget = map[string]uint64{"node-b": 1}
	replicator.refreshReplicationLagLocked()
	replicator.mu.Unlock()

	status := replicator.RegionReplicationStatus()
	if !status.Configured {
		t.Fatal("RegionReplicationStatus().Configured = false, want true")
	}
	if got := status.CurrentMaxRPOLagSequences; got != 2 {
		t.Fatalf("CurrentMaxRPOLagSequences = %d, want 2", got)
	}
	if status.RPOWithinBudget {
		t.Fatal("RPOWithinBudget = true, want false")
	}
	if len(status.MissingRemoteRegions) != 1 || status.MissingRemoteRegions[0] != "us" {
		t.Fatalf("MissingRemoteRegions = %#v, want [us]", status.MissingRemoteRegions)
	}
}

func TestReplicationRegionPolicyValidatesRecoveryDuration(t *testing.T) {
	policy := ReplicationRegionPolicy{LocalRegion: "asia", MaxRTO: 5 * time.Second}
	if err := policy.ValidateRecoveryDuration(5 * time.Second); err != nil {
		t.Fatalf("ValidateRecoveryDuration(within budget) error = %v", err)
	}
	if err := policy.ValidateRecoveryDuration(5*time.Second + time.Nanosecond); err == nil {
		t.Fatal("ValidateRecoveryDuration(over budget) error = nil")
	}
}

func TestReplicationRegionTopologyGRPCRoundTrip(t *testing.T) {
	original := TopologyNode{
		ID:      "node-b",
		Address: "http://node-b",
		Region:  "europe",
	}
	roundTrip := topologyNodeFromProto(grpcTopologyNode(original))
	if roundTrip.Region != original.Region {
		t.Fatalf("round-trip region = %q, want %q", roundTrip.Region, original.Region)
	}
}
