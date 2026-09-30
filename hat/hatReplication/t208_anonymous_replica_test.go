package hatReplication

import (
	"context"
	"errors"
	"testing"
)

func TestT208AnonymousReplicaDoesNotSatisfyWriteQuorum(t *testing.T) {
	result, err := ExecuteWriteQuorumTargets(context.Background(), []QuorumTarget{
		{Node: "voter-a"},
		{Node: "anonymous-a", Anonymous: true},
	}, 1, func(_ context.Context, node string) error {
		if node == "voter-a" {
			return errors.New("voter unavailable")
		}
		return nil
	})
	if !errors.Is(err, ErrWriteQuorumUnsatisfied) {
		t.Fatalf("ExecuteWriteQuorumTargets() error = %v, want ErrWriteQuorumUnsatisfied", err)
	}
	if result.Decision.Total != 1 || result.Decision.Acknowledged != 0 {
		t.Fatalf("decision = %#v, want one voter and zero acknowledgements", result.Decision)
	}
	if len(result.Attempts) != 2 || !result.Attempts[1].Acknowledged || !result.Attempts[1].Anonymous {
		t.Fatalf("attempts = %#v, want the anonymous replica attempted and marked anonymous", result.Attempts)
	}
}

func TestT208AnonymousReplicaDoesNotSatisfyReadQuorum(t *testing.T) {
	result, err := ExecuteReadQuorumTargets(context.Background(), []QuorumTarget{
		{Node: "voter-a"},
		{Node: "anonymous-a", Anonymous: true},
	}, 1, func(_ context.Context, node string) (any, error) {
		if node == "voter-a" {
			return nil, errors.New("voter unavailable")
		}
		return "anonymous-value", nil
	}, nil)
	if !errors.Is(err, ErrReadQuorumUnsatisfied) {
		t.Fatalf("ExecuteReadQuorumTargets() error = %v, want ErrReadQuorumUnsatisfied", err)
	}
	if result.Decision.Total != 1 || result.Decision.Acknowledged != 0 {
		t.Fatalf("decision = %#v, want one voter and zero acknowledgements", result.Decision)
	}
	if len(result.Attempts) != 2 || !result.Attempts[1].Acknowledged || !result.Attempts[1].Anonymous {
		t.Fatalf("attempts = %#v, want the anonymous replica attempted and marked anonymous", result.Attempts)
	}
}

func TestT208QuorumTargetsTrimNodeNamesWithoutMutatingInput(t *testing.T) {
	targets := []QuorumTarget{{Node: " voter-a "}, {Node: "anonymous-a", Anonymous: true}}
	result, err := ExecuteWriteQuorumTargets(context.Background(), targets, 1, func(_ context.Context, node string) error {
		if node == "voter-a" || node == "anonymous-a" {
			return nil
		}
		return errors.New("unexpected node")
	})
	if err != nil {
		t.Fatalf("ExecuteWriteQuorumTargets() error = %v", err)
	}
	if result.Attempts[0].Node != "voter-a" {
		t.Fatalf("normalized node = %q, want voter-a", result.Attempts[0].Node)
	}
	if targets[0].Node != " voter-a " {
		t.Fatalf("input target was mutated to %q", targets[0].Node)
	}
}
