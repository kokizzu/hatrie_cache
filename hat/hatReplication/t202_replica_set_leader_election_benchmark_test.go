package hatReplication

import (
	"testing"
	"time"
)

func benchmarkT202Election(b *testing.B) (*ReplicaSetLeaderElection, time.Time) {
	b.Helper()
	now := time.Unix(500, 0)
	election, err := NewReplicaSetLeaderElection(ReplicaSetLeaderElectionOptions{
		Enabled:         true,
		Voters:          []string{"node-a", "node-b", "node-c"},
		ElectionTimeout: time.Minute,
	})
	if err != nil {
		b.Fatal(err)
	}
	for _, heartbeat := range []ReplicaSetLeaderHeartbeat{
		{NodeID: "node-a", AppliedSequence: 10, Healthy: true},
		{NodeID: "node-b", AppliedSequence: 12, Healthy: true},
		{NodeID: "node-c", AppliedSequence: 11, Healthy: true},
	} {
		if err := election.ObserveHeartbeat(now, heartbeat); err != nil {
			b.Fatal(err)
		}
	}
	return election, now
}

func BenchmarkT202ExistingAutomaticFailoverEvaluation(b *testing.B) {
	options := AutomaticFailoverOptions{Enabled: true, MaxCandidates: DefaultAutomaticFailoverMaxCandidates}
	observation := AutomaticFailoverObservation{
		SourceID:           "source",
		SourceSequence:     12,
		FencingToken:       4,
		TopologyGeneration: 9,
		VoterCount:         3,
		HealthyVoterCount:  3,
		Candidates: []AutomaticFailoverCandidate{
			{NodeID: "node-a", Healthy: true, AppliedSequence: 10},
			{NodeID: "node-b", Healthy: true, AppliedSequence: 12},
			{NodeID: "node-c", Healthy: true, AppliedSequence: 11},
		},
	}
	b.ResetTimer()
	for range b.N {
		if _, err := evaluateAutomaticFailover(options, observation); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkT202ReplicaSetLeaderElectionTryElectCancel(b *testing.B) {
	election, now := benchmarkT202Election(b)
	b.ResetTimer()
	for range b.N {
		proposal, err := election.TryElect(now)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := election.Cancel(proposal); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkT202ReplicaSetLeaderElectionObserveHeartbeat(b *testing.B) {
	election, now := benchmarkT202Election(b)
	b.ResetTimer()
	for index := range b.N {
		err := election.ObserveHeartbeat(now.Add(time.Duration(index+1)*time.Nanosecond), ReplicaSetLeaderHeartbeat{
			NodeID:          "node-a",
			AppliedSequence: uint64(10 + index),
			Healthy:         true,
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
