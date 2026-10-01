package hatTopology

import "testing"

func TestPlanFailoverSupportsDeduplicatedLargeOwnerSets(t *testing.T) {
	request := FailoverRequest{
		Shard: TopologyShard{
			ID:      9,
			Primary: "node-a",
			Replicas: []string{
				"node-b", "node-c", "node-d", "node-e", "node-f",
				"node-g", "node-h", "node-i", "node-j", "node-b",
			},
		},
		CurrentLeader:        "node-a",
		Active:               map[string]bool{"node-b": true, "node-c": true, "node-d": true, "node-e": true, "node-f": true},
		AppliedSequences:     map[string]uint64{"node-b": 7},
		SourceSequence:       7,
		ExpectedFencingToken: 4,
		ObservedFencingToken: 4,
	}
	decision, err := PlanFailover(FailoverPolicy{Mode: FailoverModeAutomatic, RequiredHealthy: 5, RequireFencingToken: true}, request)
	if err != nil {
		t.Fatalf("PlanFailover() error = %v", err)
	}
	if !decision.Allowed || decision.To != "node-b" || decision.Healthy != 5 || decision.RequiredHealthy != 5 {
		t.Fatalf("decision = %+v, want deduplicated majority promotion", decision)
	}
}
