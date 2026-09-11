package hatTopology

import (
	"errors"
	"reflect"
	"testing"
)

func TestEvaluatePartitionOwnershipConsensusBindsExactMetadata(t *testing.T) {
	expected := PartitionOwnership{
		ShardID:             7,
		Primary:             "node-a",
		Replicas:            []string{"node-b", "node-c"},
		TopologyFingerprint: "topology-v7",
		FencingToken:        12,
	}
	policy := TopologyConsensusPolicy{Voters: []string{"node-c", "node-a", "node-b"}}
	votes := []PartitionOwnershipConsensusVote{
		{NodeID: "node-c", Accepted: true, Ownership: expected},
		{NodeID: "node-a", Accepted: true, Ownership: expected},
		{NodeID: "node-b", Accepted: true, Ownership: PartitionOwnership{
			ShardID:             expected.ShardID,
			Primary:             "node-old",
			Replicas:            append([]string(nil), expected.Replicas...),
			TopologyFingerprint: expected.TopologyFingerprint,
			FencingToken:        expected.FencingToken,
		}},
	}

	decision, err := EvaluatePartitionOwnershipConsensus(policy, expected, votes)
	if err != nil {
		t.Fatalf("EvaluatePartitionOwnershipConsensus() error = %v", err)
	}
	wantAcknowledged := []string{"node-a", "node-c"}
	wantRejected := []string{"node-b"}
	if !reflect.DeepEqual(decision.Acknowledged, wantAcknowledged) {
		t.Fatalf("acknowledged = %#v, want %#v", decision.Acknowledged, wantAcknowledged)
	}
	if !reflect.DeepEqual(decision.Rejected, wantRejected) {
		t.Fatalf("rejected = %#v, want %#v", decision.Rejected, wantRejected)
	}
	if !decision.Satisfied || decision.Required != 2 {
		t.Fatalf("decision = %#v, want satisfied strict majority", decision)
	}
	if err := ValidatePartitionOwnershipConsensusDecision(decision, expected); err != nil {
		t.Fatalf("ValidatePartitionOwnershipConsensusDecision() error = %v", err)
	}
}

func TestEvaluatePartitionOwnershipConsensusRejectsAnyOwnershipMetadataMismatch(t *testing.T) {
	expected := PartitionOwnership{
		ShardID:             3,
		Primary:             "node-a",
		Replicas:            []string{"node-b", "node-c"},
		TopologyFingerprint: "topology-v3",
		FencingToken:        8,
	}
	variants := []PartitionOwnership{
		{ShardID: 3, Primary: "node-a", Replicas: []string{"node-c", "node-b"}, TopologyFingerprint: "topology-v3", FencingToken: 8},
		{ShardID: 3, Primary: "node-a", Replicas: []string{"node-b", "node-c"}, TopologyFingerprint: "topology-v2", FencingToken: 8},
		{ShardID: 3, Primary: "node-a", Replicas: []string{"node-b", "node-c"}, TopologyFingerprint: "topology-v3", FencingToken: 7},
		{ShardID: 4, Primary: "node-a", Replicas: []string{"node-b", "node-c"}, TopologyFingerprint: "topology-v3", FencingToken: 8},
	}
	for index, ownership := range variants {
		decision, err := EvaluatePartitionOwnershipConsensus(
			TopologyConsensusPolicy{Voters: []string{"node-a", "node-b"}, Required: 2},
			expected,
			[]PartitionOwnershipConsensusVote{
				{NodeID: "node-a", Accepted: true, Ownership: expected},
				{NodeID: "node-b", Accepted: true, Ownership: ownership},
			},
		)
		if err != nil {
			t.Fatalf("variant %d: EvaluatePartitionOwnershipConsensus() error = %v", index, err)
		}
		if decision.Satisfied {
			t.Fatalf("variant %d: mismatched metadata satisfied consensus: %#v", index, decision)
		}
	}
}

func TestEvaluatePartitionOwnershipConsensusIsDeterministicAndDoesNotMutateInputs(t *testing.T) {
	expected := PartitionOwnership{ShardID: 1, Primary: "a", Replicas: []string{"b"}, TopologyFingerprint: "fp", FencingToken: 2}
	policy := TopologyConsensusPolicy{Voters: []string{"c", "a", "b"}, Required: 2}
	votes := []PartitionOwnershipConsensusVote{
		{NodeID: "c", Accepted: false, Ownership: expected},
		{NodeID: "b", Accepted: true, Ownership: expected},
		{NodeID: "a", Accepted: true, Ownership: expected},
	}
	policyBefore := append([]string(nil), policy.Voters...)
	votesBefore := append([]PartitionOwnershipConsensusVote(nil), votes...)
	first, err := EvaluatePartitionOwnershipConsensus(policy, expected, votes)
	if err != nil {
		t.Fatalf("first evaluation error = %v", err)
	}
	second, err := EvaluatePartitionOwnershipConsensus(policy, expected, votes)
	if err != nil {
		t.Fatalf("second evaluation error = %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("decisions differ: first=%#v second=%#v", first, second)
	}
	if !reflect.DeepEqual(policy.Voters, policyBefore) || !reflect.DeepEqual(votes, votesBefore) {
		t.Fatal("consensus evaluation mutated caller inputs")
	}
	expected.Replicas[0] = "mutated"
	if first.Ownership.Replicas[0] != "b" {
		t.Fatalf("decision ownership aliases expected replicas: %#v", first.Ownership.Replicas)
	}
}

func TestPartitionOwnershipConsensusRejectsMalformedVotesAndDecisionReuse(t *testing.T) {
	expected := PartitionOwnership{ShardID: 1, Primary: "a", TopologyFingerprint: "fp"}
	basePolicy := TopologyConsensusPolicy{Voters: []string{"a", "b"}, Required: 2}
	tests := []struct {
		name  string
		votes []PartitionOwnershipConsensusVote
		want  error
	}{
		{name: "unknown voter", votes: []PartitionOwnershipConsensusVote{{NodeID: "x", Ownership: expected}}, want: ErrPartitionOwnershipConsensusInvalid},
		{name: "duplicate voter", votes: []PartitionOwnershipConsensusVote{{NodeID: "a", Ownership: expected}, {NodeID: "a", Ownership: expected}}, want: ErrPartitionOwnershipConsensusInvalid},
		{name: "empty primary", votes: []PartitionOwnershipConsensusVote{{NodeID: "a", Ownership: PartitionOwnership{ShardID: 1, TopologyFingerprint: "fp"}}}, want: ErrPartitionOwnershipConsensusInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := EvaluatePartitionOwnershipConsensus(basePolicy, expected, test.votes)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}

	decision, err := EvaluatePartitionOwnershipConsensus(basePolicy, expected, []PartitionOwnershipConsensusVote{
		{NodeID: "a", Accepted: true, Ownership: expected},
		{NodeID: "b", Accepted: true, Ownership: expected},
	})
	if err != nil {
		t.Fatalf("valid evaluation error = %v", err)
	}
	wrong := expected
	wrong.FencingToken++
	if err := ValidatePartitionOwnershipConsensusDecision(decision, wrong); !errors.Is(err, ErrPartitionOwnershipConsensusInvalid) {
		t.Fatalf("wrong expected ownership error = %v, want ErrPartitionOwnershipConsensusInvalid", err)
	}
	decision.Acknowledged = append(decision.Acknowledged, decision.Acknowledged[0])
	if err := ValidatePartitionOwnershipConsensusDecision(decision, expected); !errors.Is(err, ErrPartitionOwnershipConsensusInvalid) {
		t.Fatalf("duplicate acknowledgement error = %v, want ErrPartitionOwnershipConsensusInvalid", err)
	}
}

func TestValidatePartitionOwnershipConsensusDecisionRequiresQuorum(t *testing.T) {
	expected := PartitionOwnership{ShardID: 1, Primary: "a", TopologyFingerprint: "fp"}
	decision, err := EvaluatePartitionOwnershipConsensus(
		TopologyConsensusPolicy{Voters: []string{"a", "b", "c"}},
		expected,
		[]PartitionOwnershipConsensusVote{{NodeID: "a", Accepted: true, Ownership: expected}},
	)
	if err != nil {
		t.Fatalf("evaluation error = %v", err)
	}
	if decision.Satisfied {
		t.Fatal("decision unexpectedly satisfied quorum")
	}
	if err := ValidatePartitionOwnershipConsensusDecision(decision, expected); !errors.Is(err, ErrPartitionOwnershipConsensusUnsatisfied) {
		t.Fatalf("validation error = %v, want ErrPartitionOwnershipConsensusUnsatisfied", err)
	}
}
