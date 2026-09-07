package hatReplication

import "errors"

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
	return SelectReadReplicaWithConsistency(candidates, policy, ReadConsistencyReadAfterWrite)
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
