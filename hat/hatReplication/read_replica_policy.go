package hatReplication

import (
	"errors"
	"strings"
)

var (
	ErrReadReplicaNameRequired = errors.New("hatriecache: read replica name is required")
	ErrNoEligibleReadReplica   = errors.New("hatriecache: no eligible read replica")
)

// ReadReplicaProgress describes the freshness and health of one read replica.
type ReadReplicaProgress struct {
	Node        string
	Region      string
	Frontier    uint64
	HealthScore int
}

// ReadReplicaPolicy bounds the staleness accepted for a read.
type ReadReplicaPolicy struct {
	ObservedFrontier uint64
	RequiredFrontier uint64
	MaxLag           uint64
	// PreferredRegions orders optional locality preferences. A matching region
	// is preferred after consistency filtering, with fallback to any eligible
	// region when none of the preferences are available.
	PreferredRegions []string
}

// SelectReadReplica chooses an eligible replica without mutating candidates.
// Configured locality is preferred first, followed by freshness, health, and
// lexical node name. A zero MaxLag therefore requires a replica at or ahead of
// ObservedFrontier.
func SelectReadReplica(candidates []ReadReplicaProgress, policy ReadReplicaPolicy) (ReadReplicaProgress, error) {
	return SelectReadReplicaWithConsistency(candidates, policy, ReadConsistencyReadAfterWrite)
}

func readReplicaPreferred(candidate, selected ReadReplicaProgress, policy ReadReplicaPolicy) bool {
	if len(policy.PreferredRegions) > 0 {
		candidateRegion := readReplicaRegionRank(candidate.Region, policy.PreferredRegions)
		selectedRegion := readReplicaRegionRank(selected.Region, policy.PreferredRegions)
		if candidateRegion != selectedRegion {
			return candidateRegion < selectedRegion
		}
	}
	return readReplicaPreferredByFreshness(candidate, selected)
}

func readReplicaPreferredByFreshness(candidate, selected ReadReplicaProgress) bool {
	if candidate.Frontier != selected.Frontier {
		return candidate.Frontier > selected.Frontier
	}
	if candidate.HealthScore != selected.HealthScore {
		return candidate.HealthScore > selected.HealthScore
	}
	return candidate.Node < selected.Node
}

func readReplicaRegionRank(region string, preferred []string) int {
	region = strings.TrimSpace(region)
	for index, preferredRegion := range preferred {
		preferredRegion = strings.TrimSpace(preferredRegion)
		if preferredRegion != "" && strings.EqualFold(region, preferredRegion) {
			return index
		}
	}
	return len(preferred)
}
