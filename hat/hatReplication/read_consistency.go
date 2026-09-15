package hatReplication

import (
	"errors"
	"fmt"
	"strings"
)

var ErrReadConsistencyInvalid = errors.New("hatriecache: read consistency level is invalid")

// ReadConsistencyLevel controls which freshness constraints are applied when
// selecting a read replica.
type ReadConsistencyLevel string

const (
	// ReadConsistencyEventual permits any healthy candidate.
	ReadConsistencyEventual ReadConsistencyLevel = "eventual"
	// ReadConsistencyBoundedStaleness enforces ReadReplicaPolicy.MaxLag.
	ReadConsistencyBoundedStaleness ReadConsistencyLevel = "bounded-staleness"
	// ReadConsistencyReadAfterWrite enforces RequiredFrontier and MaxLag.
	ReadConsistencyReadAfterWrite ReadConsistencyLevel = "read-after-write"
)

// ParseReadConsistencyLevel parses the stable configuration spelling. An
// empty value selects the backward-compatible read-after-write policy.
func ParseReadConsistencyLevel(value string) (ReadConsistencyLevel, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return ReadConsistencyReadAfterWrite, nil
	case string(ReadConsistencyEventual):
		return ReadConsistencyEventual, nil
	case string(ReadConsistencyBoundedStaleness), "bounded":
		return ReadConsistencyBoundedStaleness, nil
	case string(ReadConsistencyReadAfterWrite), "session":
		return ReadConsistencyReadAfterWrite, nil
	default:
		return ReadConsistencyReadAfterWrite, fmt.Errorf("%w: %q", ErrReadConsistencyInvalid, value)
	}
}

// SelectReadReplicaWithConsistency selects an eligible candidate under an
// explicit freshness level. Configured locality is preferred first, followed by
// highest frontier, health score, and lexical node name.
func SelectReadReplicaWithConsistency(candidates []ReadReplicaProgress, policy ReadReplicaPolicy, level ReadConsistencyLevel) (ReadReplicaProgress, error) {
	switch level {
	case ReadConsistencyEventual, ReadConsistencyBoundedStaleness, ReadConsistencyReadAfterWrite:
	default:
		return ReadReplicaProgress{}, fmt.Errorf("%w: %q", ErrReadConsistencyInvalid, level)
	}
	if len(candidates) == 1 {
		candidate := candidates[0]
		node := strings.TrimSpace(candidate.Node)
		if node == "" {
			return ReadReplicaProgress{}, ErrReadReplicaNameRequired
		}
		candidate.Node = node
		lag := uint64(0)
		if policy.ObservedFrontier > candidate.Frontier {
			lag = policy.ObservedFrontier - candidate.Frontier
		}
		switch level {
		case ReadConsistencyBoundedStaleness:
			if lag > policy.MaxLag {
				return ReadReplicaProgress{}, noEligibleReadReplicaError(policy, level)
			}
		case ReadConsistencyReadAfterWrite:
			if candidate.Frontier < policy.RequiredFrontier || lag > policy.MaxLag {
				return ReadReplicaProgress{}, noEligibleReadReplicaError(policy, level)
			}
		}
		return candidate, nil
	}
	var selected ReadReplicaProgress
	found := false
	preferredRegionCount := len(policy.PreferredRegions)
	selectedRegionRank := preferredRegionCount
	for _, candidate := range candidates {
		node := strings.TrimSpace(candidate.Node)
		if node == "" {
			return ReadReplicaProgress{}, ErrReadReplicaNameRequired
		}
		candidate.Node = node
		lag := uint64(0)
		if policy.ObservedFrontier > candidate.Frontier {
			lag = policy.ObservedFrontier - candidate.Frontier
		}
		switch level {
		case ReadConsistencyBoundedStaleness:
			if lag > policy.MaxLag {
				continue
			}
		case ReadConsistencyReadAfterWrite:
			if candidate.Frontier < policy.RequiredFrontier || lag > policy.MaxLag {
				continue
			}
		}
		candidateRegionRank := preferredRegionCount
		if preferredRegionCount > 0 {
			candidateRegionRank = readReplicaRegionRank(candidate.Region, policy.PreferredRegions)
		}
		if !found {
			selected = candidate
			selectedRegionRank = candidateRegionRank
			found = true
			continue
		}
		if preferredRegionCount > 0 && candidateRegionRank != selectedRegionRank {
			if candidateRegionRank < selectedRegionRank {
				selected = candidate
				selectedRegionRank = candidateRegionRank
			}
			continue
		}
		if readReplicaPreferredByFreshness(candidate, selected) {
			selected = candidate
			selectedRegionRank = candidateRegionRank
		}
	}
	if !found {
		return ReadReplicaProgress{}, noEligibleReadReplicaError(policy, level)
	}
	return selected, nil
}

func noEligibleReadReplicaError(policy ReadReplicaPolicy, level ReadConsistencyLevel) error {
	return fmt.Errorf("%w: consistency=%s required_frontier=%d observed_frontier=%d max_lag=%d", ErrNoEligibleReadReplica, level, policy.RequiredFrontier, policy.ObservedFrontier, policy.MaxLag)
}
