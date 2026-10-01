package hatTopology

import "testing"

func BenchmarkFailoverExistingElectionControl(b *testing.B) {
	shard := TopologyShard{ID: 7, Primary: "node-a", Replicas: []string{"node-b", "node-c"}}
	active := map[string]bool{"node-a": false, "node-b": true, "node-c": true}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		decision := ElectShardLeader(shard, active)
		if !decision.Available || decision.Leader != "node-b" {
			b.Fatal(decision)
		}
	}
}

func BenchmarkPlanFailoverAutomatic(b *testing.B) {
	request := failoverTestRequest()
	policy := FailoverPolicy{Mode: FailoverModeAutomatic, RequireFencingToken: true}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		decision, err := PlanFailover(policy, request)
		if err != nil || !decision.Allowed || decision.To != "node-b" {
			b.Fatalf("decision=%+v err=%v", decision, err)
		}
	}
}

func BenchmarkPlanFailoverDisabled(b *testing.B) {
	request := failoverTestRequest()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		decision, err := PlanFailover(FailoverPolicy{}, request)
		if err != nil || decision.Allowed || decision.Reason != failoverReasonDisabled {
			b.Fatalf("decision=%+v err=%v", decision, err)
		}
	}
}
