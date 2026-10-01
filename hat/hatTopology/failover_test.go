package hatTopology

import (
	"errors"
	"testing"
)

func failoverTestRequest() FailoverRequest {
	return FailoverRequest{
		Shard:                TopologyShard{ID: 7, Primary: "node-a", Replicas: []string{"node-b", "node-c"}},
		CurrentLeader:        "node-a",
		Active:               map[string]bool{"node-a": false, "node-b": true, "node-c": true},
		AppliedSequences:     map[string]uint64{"node-b": 100, "node-c": 99},
		FailureDomains:       map[string]string{"node-a": "zone-a", "node-b": "zone-b", "node-c": "zone-c"},
		SourceSequence:       100,
		ExpectedFencingToken: 9,
		ObservedFencingToken: 9,
		OperatorApproved:     false,
	}
}

func TestPlanFailoverIsDisabledByDefault(t *testing.T) {
	decision, err := PlanFailover(FailoverPolicy{}, failoverTestRequest())
	if err != nil {
		t.Fatalf("PlanFailover() error = %v", err)
	}
	if decision.Allowed || decision.Reason != "disabled" {
		t.Fatalf("decision = %#v, want disabled decision", decision)
	}
}

func TestPlanFailoverRequiresQuorumCatchupAndFencing(t *testing.T) {
	request := failoverTestRequest()
	decision, err := PlanFailover(FailoverPolicy{Mode: FailoverModeAutomatic, RequireFencingToken: true}, request)
	if err != nil {
		t.Fatalf("PlanFailover() error = %v", err)
	}
	if !decision.Allowed || decision.From != "node-a" || decision.To != "node-b" || decision.Healthy != 2 || decision.RequiredHealthy != 2 || decision.FailureDomains != 2 || decision.AppliedSequence != 100 || decision.FencingToken != 9 {
		t.Fatalf("decision = %#v, want deterministic quorum-approved promotion", decision)
	}

	request.Active["node-c"] = false
	if _, err := PlanFailover(FailoverPolicy{Mode: FailoverModeAutomatic}, request); !errors.Is(err, ErrFailoverQuorumUnsatisfied) {
		t.Fatalf("quorum error = %v, want ErrFailoverQuorumUnsatisfied", err)
	}
	request.Active["node-c"] = true
	request.AppliedSequences["node-b"] = 99
	request.AppliedSequences["node-c"] = 98
	if _, err := PlanFailover(FailoverPolicy{Mode: FailoverModeAutomatic}, request); !errors.Is(err, ErrFailoverCandidateNotCaughtUp) {
		t.Fatalf("catch-up error = %v, want ErrFailoverCandidateNotCaughtUp", err)
	}
	request.AppliedSequences["node-b"] = 100
	request.ObservedFencingToken = 8
	if _, err := PlanFailover(FailoverPolicy{Mode: FailoverModeAutomatic, RequireFencingToken: true}, request); !errors.Is(err, ErrFailoverFencingMismatch) {
		t.Fatalf("fencing error = %v, want ErrFailoverFencingMismatch", err)
	}
}

func TestPlanFailoverRequiresExplicitOperatorApprovalAndFailureDomains(t *testing.T) {
	request := failoverTestRequest()
	if _, err := PlanFailover(FailoverPolicy{Mode: FailoverModeOperatorApproved}, request); !errors.Is(err, ErrFailoverOperatorApprovalRequired) {
		t.Fatalf("approval error = %v, want ErrFailoverOperatorApprovalRequired", err)
	}
	request.OperatorApproved = true
	decision, err := PlanFailover(FailoverPolicy{Mode: FailoverModeOperatorApproved, MinFailureDomains: 2}, request)
	if err != nil || !decision.Allowed {
		t.Fatalf("approved PlanFailover() = %#v/%v, want allowed", decision, err)
	}

	request.FailureDomains["node-c"] = "zone-b"
	if _, err := PlanFailover(FailoverPolicy{Mode: FailoverModeOperatorApproved, MinFailureDomains: 3}, request); !errors.Is(err, ErrFailoverFailureDomainUnsatisfied) {
		t.Fatalf("failure-domain error = %v, want ErrFailoverFailureDomainUnsatisfied", err)
	}
}

func TestPlanFailoverPreservesHealthyLeaderAndValidatesPolicy(t *testing.T) {
	request := failoverTestRequest()
	request.Active["node-a"] = true
	decision, err := PlanFailover(FailoverPolicy{Mode: FailoverModeAutomatic}, request)
	if err != nil || decision.Allowed || decision.Reason != "leader_healthy" {
		t.Fatalf("healthy leader decision = %#v/%v, want no-op", decision, err)
	}

	if _, err := PlanFailover(FailoverPolicy{Mode: FailoverMode(99)}, request); !errors.Is(err, ErrFailoverPolicyInvalid) {
		t.Fatalf("invalid mode error = %v, want ErrFailoverPolicyInvalid", err)
	}
	if _, err := PlanFailover(FailoverPolicy{Mode: FailoverModeAutomatic, RequiredHealthy: 4}, request); !errors.Is(err, ErrFailoverPolicyInvalid) {
		t.Fatalf("invalid quorum error = %v, want ErrFailoverPolicyInvalid", err)
	}
}
