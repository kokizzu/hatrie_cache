package hatReplication

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrReadReplicaNameRequired = errors.New("hatriecache: read replica name is required")
	ErrNoEligibleReadReplica   = errors.New("hatriecache: no eligible read replica")
)

// ReadReplicaProgress describes the freshness and health of one read replica.
type ReadReplicaProgress struct {
	Node        string
	Frontier    uint64
	HealthScore int
}

// ReadReplicaPolicy bounds the staleness accepted for a read.
type ReadReplicaPolicy struct {
	ObservedFrontier uint64
	RequiredFrontier uint64
	MaxLag           uint64
}

// SelectReadReplica chooses an eligible replica without mutating candidates.
// Freshness is preferred first, then health, then lexical node name. A zero
// MaxLag therefore requires a replica at or ahead of ObservedFrontier.
func SelectReadReplica(candidates []ReadReplicaProgress, policy ReadReplicaPolicy) (ReadReplicaProgress, error) {
	var selected ReadReplicaProgress
	found := false
	for _, candidate := range candidates {
		node := strings.TrimSpace(candidate.Node)
		if node == "" {
			return ReadReplicaProgress{}, ErrReadReplicaNameRequired
		}
		candidate.Node = node
		if candidate.Frontier < policy.RequiredFrontier {
			continue
		}
		lag := uint64(0)
		if policy.ObservedFrontier > candidate.Frontier {
			lag = policy.ObservedFrontier - candidate.Frontier
		}
		if lag > policy.MaxLag {
			continue
		}
		if !found || readReplicaPreferred(candidate, selected) {
			selected = candidate
			found = true
		}
	}
	if !found {
		return ReadReplicaProgress{}, fmt.Errorf("%w: required_frontier=%d observed_frontier=%d max_lag=%d", ErrNoEligibleReadReplica, policy.RequiredFrontier, policy.ObservedFrontier, policy.MaxLag)
	}
	return selected, nil
}

func readReplicaPreferred(candidate, selected ReadReplicaProgress) bool {
	if candidate.Frontier != selected.Frontier {
		return candidate.Frontier > selected.Frontier
	}
	if candidate.HealthScore != selected.HealthScore {
		return candidate.HealthScore > selected.HealthScore
	}
	return candidate.Node < selected.Node
}
