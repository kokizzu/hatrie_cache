package hatTopology

import (
	"errors"
	"testing"
)

func TestC153dAuthenticatedPartitionOwnershipVotes(t *testing.T) {
	authenticator, err := NewPartitionOwnershipConsensusAuthenticator("cluster-key-v1", []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("NewPartitionOwnershipConsensusAuthenticator() error = %v", err)
	}
	expected := c153dTestOwnership()
	policy := TopologyConsensusPolicy{Voters: []string{"node-a", "node-b", "node-c"}, Required: 2}
	votes := make([]PartitionOwnershipConsensusVote, 0, 2)
	for _, nodeID := range []string{"node-a", "node-b"} {
		vote, err := authenticator.Sign(PartitionOwnershipConsensusVote{NodeID: nodeID, Ownership: expected, Accepted: true})
		if err != nil {
			t.Fatalf("Sign(%q) error = %v", nodeID, err)
		}
		if vote.KeyID != "cluster-key-v1" || len(vote.Signature) == 0 {
			t.Fatalf("signed vote = %#v, want key id and signature", vote)
		}
		if err := authenticator.Verify(vote); err != nil {
			t.Fatalf("Verify(%q) error = %v", nodeID, err)
		}
		votes = append(votes, vote)
	}
	decision, err := EvaluateAuthenticatedPartitionOwnershipConsensus(policy, expected, votes, authenticator)
	if err != nil {
		t.Fatalf("EvaluateAuthenticatedPartitionOwnershipConsensus() error = %v", err)
	}
	if !decision.Satisfied || len(decision.Acknowledged) != 2 {
		t.Fatalf("decision = %#v, want satisfied with two acknowledgements", decision)
	}
	if err := ValidatePartitionOwnershipConsensusDecision(decision, expected); err != nil {
		t.Fatalf("ValidatePartitionOwnershipConsensusDecision() error = %v", err)
	}
}

func TestC153dRejectsTamperedAndUnsignedVotes(t *testing.T) {
	authenticator, err := NewPartitionOwnershipConsensusAuthenticator("cluster-key-v1", []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("NewPartitionOwnershipConsensusAuthenticator() error = %v", err)
	}
	expected := c153dTestOwnership()
	policy := TopologyConsensusPolicy{Voters: []string{"node-a", "node-b"}, Required: 1}
	vote, err := authenticator.Sign(PartitionOwnershipConsensusVote{NodeID: "node-a", Ownership: expected, Accepted: true})
	if err != nil {
		t.Fatalf("Sign() error = %v", err)
	}
	for name, mutate := range map[string]func(PartitionOwnershipConsensusVote) PartitionOwnershipConsensusVote{
		"accepted flag": func(value PartitionOwnershipConsensusVote) PartitionOwnershipConsensusVote {
			value.Accepted = false
			return value
		},
		"ownership": func(value PartitionOwnershipConsensusVote) PartitionOwnershipConsensusVote {
			value.Ownership.FencingToken++
			return value
		},
		"node": func(value PartitionOwnershipConsensusVote) PartitionOwnershipConsensusVote {
			value.NodeID = "node-b"
			return value
		},
		"key id": func(value PartitionOwnershipConsensusVote) PartitionOwnershipConsensusVote {
			value.KeyID = "old-key"
			return value
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := EvaluateAuthenticatedPartitionOwnershipConsensus(policy, expected, []PartitionOwnershipConsensusVote{mutate(vote)}, authenticator)
			if !errors.Is(err, ErrPartitionOwnershipConsensusAuthentication) {
				t.Fatalf("error = %v, want authentication error", err)
			}
		})
	}
	unsigned := PartitionOwnershipConsensusVote{NodeID: "node-a", Ownership: expected, Accepted: true}
	if _, err := EvaluateAuthenticatedPartitionOwnershipConsensus(policy, expected, []PartitionOwnershipConsensusVote{unsigned}, authenticator); !errors.Is(err, ErrPartitionOwnershipConsensusAuthentication) {
		t.Fatalf("unsigned error = %v, want authentication error", err)
	}
	if _, err := EvaluatePartitionOwnershipConsensus(policy, expected, []PartitionOwnershipConsensusVote{unsigned}); err != nil {
		t.Fatalf("legacy evaluator error = %v, want compatibility", err)
	}
}

func TestC153dAuthenticatorValidation(t *testing.T) {
	for name, keyID := range map[string]string{
		"empty key id": "",
		"space key id": " key-v1",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewPartitionOwnershipConsensusAuthenticator(keyID, []byte("0123456789abcdef0123456789abcdef")); !errors.Is(err, ErrPartitionOwnershipConsensusAuthentication) {
				t.Fatalf("error = %v, want authentication error", err)
			}
		})
	}
	if _, err := NewPartitionOwnershipConsensusAuthenticator("key-v1", []byte("short")); !errors.Is(err, ErrPartitionOwnershipConsensusAuthentication) {
		t.Fatalf("short key error = %v, want authentication error", err)
	}
}

func BenchmarkC153dPartitionOwnershipConsensus(b *testing.B) {
	authenticator, err := NewPartitionOwnershipConsensusAuthenticator("cluster-key-v1", []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		b.Fatal(err)
	}
	expected := c153dTestOwnership()
	policy := TopologyConsensusPolicy{Voters: []string{"node-a", "node-b", "node-c"}, Required: 2}
	votes := make([]PartitionOwnershipConsensusVote, 0, 3)
	for _, nodeID := range policy.Voters {
		vote, err := authenticator.Sign(PartitionOwnershipConsensusVote{NodeID: nodeID, Ownership: expected, Accepted: true})
		if err != nil {
			b.Fatal(err)
		}
		votes = append(votes, vote)
	}
	b.Run("legacy", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := EvaluatePartitionOwnershipConsensus(policy, expected, votes); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("authenticated", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := EvaluateAuthenticatedPartitionOwnershipConsensus(policy, expected, votes, authenticator); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func c153dTestOwnership() PartitionOwnership {
	return PartitionOwnership{
		ShardID:             7,
		Primary:             "node-a",
		Replicas:            []string{"node-b"},
		TopologyFingerprint: "topology-v7",
		FencingToken:        42,
	}
}
