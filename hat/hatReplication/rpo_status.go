package hatReplication

import "errors"

var ErrReplicaRPOStatusLimit = errors.New("hatriecache: replica RPO status input exceeds the limit")

const MaxReplicaRPOStatuses = 4096

// ReplicaRPOInput carries the applied journal sequence for one replica. Node
// is an operator-facing identifier and is returned in the same order supplied.
type ReplicaRPOInput struct {
	Node            string
	AppliedSequence uint64
}

// ReplicaRPOStatus is a clock-independent point-in-time replication status.
// LagSequences is an RPO proxy: it measures unapplied journal entries rather
// than elapsed time, so clock skew cannot produce a false healthy result.
type ReplicaRPOStatus struct {
	Node               string `json:"node"`
	SourceSequence     uint64 `json:"source_sequence"`
	AppliedSequence    uint64 `json:"applied_sequence"`
	LagSequences       uint64 `json:"lag_sequences"`
	MaxRPOLagSequences uint64 `json:"max_rpo_lag_sequences,omitempty"`
	RPOWithinBudget    bool   `json:"rpo_within_budget"`
}

// BuildReplicaRPOStatus computes one bounded, underflow-safe replica status.
// A zero maxLag disables the budget check while still reporting exact lag.
func BuildReplicaRPOStatus(node string, sourceSequence, appliedSequence, maxLag uint64) ReplicaRPOStatus {
	lag := uint64(0)
	if appliedSequence < sourceSequence {
		lag = sourceSequence - appliedSequence
	}
	return ReplicaRPOStatus{
		Node:               node,
		SourceSequence:     sourceSequence,
		AppliedSequence:    appliedSequence,
		LagSequences:       lag,
		MaxRPOLagSequences: maxLag,
		RPOWithinBudget:    maxLag == 0 || lag <= maxLag,
	}
}

// BuildReplicaRPOStatuses builds statuses without maps or sorting. The input
// limit bounds memory for a monitoring snapshot and keeps caller ordering
// stable for deterministic output.
func BuildReplicaRPOStatuses(sourceSequence uint64, replicas []ReplicaRPOInput, maxLag uint64) ([]ReplicaRPOStatus, error) {
	if len(replicas) > MaxReplicaRPOStatuses {
		return nil, ErrReplicaRPOStatusLimit
	}
	statuses := make([]ReplicaRPOStatus, len(replicas))
	for index, replica := range replicas {
		statuses[index] = BuildReplicaRPOStatus(replica.Node, sourceSequence, replica.AppliedSequence, maxLag)
	}
	return statuses, nil
}
