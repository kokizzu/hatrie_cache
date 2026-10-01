package hatTopology

import "errors"

var (
	// ErrFailoverPolicyInvalid reports an invalid failover mode or threshold.
	ErrFailoverPolicyInvalid = errors.New("hatriecache: failover policy is invalid")
	// ErrFailoverOperatorApprovalRequired reports a promotion without the
	// explicit approval required by operator-controlled failover.
	ErrFailoverOperatorApprovalRequired = errors.New("hatriecache: failover operator approval is required")
	// ErrFailoverQuorumUnsatisfied reports too few healthy shard owners.
	ErrFailoverQuorumUnsatisfied = errors.New("hatriecache: failover quorum is unsatisfied")
	// ErrFailoverNoCandidate reports that no healthy promotion candidate exists.
	ErrFailoverNoCandidate = errors.New("hatriecache: failover candidate is unavailable")
	// ErrFailoverCandidateNotCaughtUp reports a candidate behind the source.
	ErrFailoverCandidateNotCaughtUp = errors.New("hatriecache: failover candidate is not caught up")
	// ErrFailoverFencingMismatch reports a missing or stale fencing token.
	ErrFailoverFencingMismatch = errors.New("hatriecache: failover fencing token mismatch")
	// ErrFailoverFailureDomainUnsatisfied reports insufficient failure-domain
	// diversity among healthy shard owners.
	ErrFailoverFailureDomainUnsatisfied = errors.New("hatriecache: failover failure-domain requirement is unsatisfied")
)

// FailoverMode selects how a failover plan may be admitted. The zero value is
// disabled so adding a policy without explicitly opting in cannot promote a
// node.
type FailoverMode uint8

const (
	FailoverModeDisabled FailoverMode = iota
	FailoverModeOperatorApproved
	FailoverModeAutomatic
)

// FailoverPolicy contains the safety gates for a promotion plan.
//
// RequiredHealthy and MinFailureDomains use zero to select their safe
// defaults. A zero RequiredHealthy means a majority of shard owners. A zero
// MinFailureDomains means that failure-domain diversity is not required.
type FailoverPolicy struct {
	Mode                FailoverMode
	RequiredHealthy     int
	MinFailureDomains   int
	RequireFencingToken bool
}

// FailoverRequest is a caller-owned, point-in-time view of a shard and its
// health. PlanFailover only reads this data; it does not mutate topology,
// fencing state, replication state, or cache data.
type FailoverRequest struct {
	Shard                TopologyShard
	CurrentLeader        string
	Active               map[string]bool
	AppliedSequences     map[string]uint64
	FailureDomains       map[string]string
	SourceSequence       uint64
	ExpectedFencingToken uint64
	ObservedFencingToken uint64
	OperatorApproved     bool
}

// FailoverDecision is the deterministic result of a failover plan.
//
// An allowed decision is still only an admission result. The caller must
// revalidate the fencing token and commit the promotion through its existing
// promotion barrier before serving writes.
type FailoverDecision struct {
	Allowed         bool
	Reason          string
	ShardID         uint32
	From            string
	To              string
	Healthy         int
	RequiredHealthy int
	FailureDomains  int
	SourceSequence  uint64
	AppliedSequence uint64
	FencingToken    uint64
}

const (
	failoverReasonDisabled       = "disabled"
	failoverReasonLeaderHealthy  = "leader_healthy"
	failoverReasonCandidateReady = "candidate_ready"
)

// PlanFailover evaluates a health-triggered promotion without performing it.
// Candidate order is primary followed by replicas, matching ElectShardLeader.
// The first healthy candidate is retained as the deterministic choice; a
// lagging preferred candidate is rejected instead of silently changing order.
func PlanFailover(policy FailoverPolicy, request FailoverRequest) (FailoverDecision, error) {
	var ownerStorage [8]string
	owners := ownerStorage[:0]
	owners = appendFailoverOwner(owners, request.Shard.Primary)
	for _, replica := range request.Shard.Replicas {
		owners = appendFailoverOwner(owners, replica)
	}
	if err := validateFailoverPolicy(policy, len(owners)); err != nil {
		return FailoverDecision{}, err
	}
	if len(owners) == 0 || request.CurrentLeader == "" || !containsFailoverOwner(owners, request.CurrentLeader) {
		return FailoverDecision{}, ErrFailoverPolicyInvalid
	}

	decision := FailoverDecision{
		ShardID:         request.Shard.ID,
		From:            request.CurrentLeader,
		RequiredHealthy: failoverRequiredHealthy(policy, len(owners)),
		SourceSequence:  request.SourceSequence,
		FencingToken:    request.ObservedFencingToken,
	}
	if policy.Mode == FailoverModeDisabled {
		decision.Reason = failoverReasonDisabled
		return decision, nil
	}
	if request.Active[request.CurrentLeader] {
		decision.Reason = failoverReasonLeaderHealthy
		return decision, nil
	}

	if policy.Mode == FailoverModeOperatorApproved && !request.OperatorApproved {
		return decision, ErrFailoverOperatorApprovalRequired
	}

	for _, owner := range owners {
		if request.Active[owner] {
			decision.Healthy++
		}
	}
	if decision.Healthy < decision.RequiredHealthy {
		return decision, ErrFailoverQuorumUnsatisfied
	}

	if policy.RequireFencingToken && (request.ExpectedFencingToken == 0 || request.ObservedFencingToken == 0 || request.ExpectedFencingToken != request.ObservedFencingToken) {
		return decision, ErrFailoverFencingMismatch
	}

	decision.FailureDomains = countActiveFailureDomains(owners, request.Active, request.FailureDomains)
	if policy.MinFailureDomains > 0 && decision.FailureDomains < policy.MinFailureDomains {
		return decision, ErrFailoverFailureDomainUnsatisfied
	}

	for _, owner := range owners {
		if !request.Active[owner] {
			continue
		}
		decision.To = owner
		decision.AppliedSequence = request.AppliedSequences[owner]
		if decision.AppliedSequence < request.SourceSequence {
			return decision, ErrFailoverCandidateNotCaughtUp
		}
		decision.Allowed = true
		decision.Reason = failoverReasonCandidateReady
		return decision, nil
	}
	return decision, ErrFailoverNoCandidate
}

func validateFailoverPolicy(policy FailoverPolicy, ownerCount int) error {
	if policy.Mode > FailoverModeAutomatic || policy.RequiredHealthy < 0 || policy.MinFailureDomains < 0 || ownerCount == 0 {
		return ErrFailoverPolicyInvalid
	}
	if policy.RequiredHealthy > ownerCount {
		return ErrFailoverPolicyInvalid
	}
	return nil
}

func failoverRequiredHealthy(policy FailoverPolicy, ownerCount int) int {
	if policy.RequiredHealthy > 0 {
		return policy.RequiredHealthy
	}
	return ownerCount/2 + 1
}

func appendFailoverOwner(owners []string, owner string) []string {
	if owner == "" || containsFailoverOwner(owners, owner) {
		return owners
	}
	return append(owners, owner)
}

func containsFailoverOwner(owners []string, candidate string) bool {
	for _, owner := range owners {
		if owner == candidate {
			return true
		}
	}
	return false
}

func countActiveFailureDomains(owners []string, active map[string]bool, domains map[string]string) int {
	var seen [8]string
	count := 0
	for ownerIndex, owner := range owners {
		if !active[owner] {
			continue
		}
		domain := domains[owner]
		if domain == "" {
			continue
		}
		alreadyCounted := false
		for seenIndex := 0; seenIndex < count && seenIndex < len(seen); seenIndex++ {
			if seen[seenIndex] == domain {
				alreadyCounted = true
				break
			}
		}
		if !alreadyCounted && count >= len(seen) {
			for previousIndex := 0; previousIndex < ownerIndex; previousIndex++ {
				previousOwner := owners[previousIndex]
				if active[previousOwner] && domains[previousOwner] == domain {
					alreadyCounted = true
					break
				}
			}
		}
		if !alreadyCounted {
			if count < len(seen) {
				seen[count] = domain
			}
			count++
		}
	}
	return count
}
