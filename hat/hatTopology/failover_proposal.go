package hatTopology

import (
	"errors"
	"fmt"
)

var (
	// ErrFailoverInvalid reports an invalid failover request or topology.
	ErrFailoverInvalid = errors.New("hatriecache: invalid failover proposal")
	// ErrFailoverPrimaryHealthy reports that the current primary is still
	// eligible, so a promotion would be unsafe.
	ErrFailoverPrimaryHealthy = errors.New("hatriecache: failover primary is healthy")
	// ErrFailoverNoCandidate reports that no replica can be promoted.
	ErrFailoverNoCandidate = errors.New("hatriecache: failover has no healthy replica candidate")
	// ErrFailoverInsufficientHealthyOwners reports that the local health policy
	// does not have enough owners to create a proposal.
	ErrFailoverInsufficientHealthyOwners = errors.New("hatriecache: failover healthy-owner requirement is not satisfied")
)

// FailoverOptions controls one explicit health-to-topology proposal. The
// default requires one healthy replica; callers should use
// EvaluateTopologyConsensus before publishing the returned commit.
type FailoverOptions struct {
	// MinHealthyOwners requires this many healthy owners, including the
	// candidate. Zero uses the safe minimum of one candidate.
	MinHealthyOwners int
}

// FailoverProposal is an independent candidate topology transition. It does
// not mutate the ElectionStore or the topology provider.
type FailoverProposal struct {
	ShardID               uint32
	PreviousPrimary       string
	CandidatePrimary      string
	HealthyOwners         []string
	RequiredHealthyOwners int
	Commit                TopologyCommit
}

// ProposeFailover checks one shard's current health and creates a fenced
// topology commit that promotes the first healthy replica. The current
// topology, liveness state, and proposal slices are copied; callers can
// safely collect consensus or persist the proposal after this method returns.
func (store *ElectionStore) ProposeFailover(shardID uint32, options FailoverOptions) (FailoverProposal, error) {
	if store == nil {
		return FailoverProposal{}, ErrFailoverInvalid
	}
	if options.MinHealthyOwners < 0 {
		return FailoverProposal{}, fmt.Errorf("%w: minimum healthy owners cannot be negative", ErrFailoverInvalid)
	}
	requiredHealthy := options.MinHealthyOwners
	if requiredHealthy == 0 {
		requiredHealthy = 1
	}

	topology, ok := store.topologySnapshot()
	if !ok {
		return FailoverProposal{}, fmt.Errorf("%w: topology is unavailable", ErrFailoverInvalid)
	}
	normalized, err := Normalize(topology)
	if err != nil {
		return FailoverProposal{}, fmt.Errorf("%w: topology: %v", ErrFailoverInvalid, err)
	}
	shard, exists := failoverShard(normalized, shardID)
	if !exists {
		return FailoverProposal{}, fmt.Errorf("%w: shard %d does not exist", ErrFailoverInvalid, shardID)
	}
	owners := Owners(shard)
	if len(owners) == 0 || requiredHealthy > len(owners) {
		return FailoverProposal{}, fmt.Errorf("%w: required=%d owners=%d", ErrFailoverInsufficientHealthyOwners, requiredHealthy, len(owners))
	}

	store.mu.RLock()
	active := store.activeNodesLocked(normalized, store.now())
	store.mu.RUnlock()
	if active[shard.Primary] {
		return FailoverProposal{}, fmt.Errorf("%w: shard %d primary %q", ErrFailoverPrimaryHealthy, shardID, shard.Primary)
	}
	healthyOwners := make([]string, 0, len(owners))
	candidate := ""
	for _, owner := range owners {
		if !active[owner] {
			continue
		}
		healthyOwners = append(healthyOwners, owner)
		if candidate == "" && owner != shard.Primary {
			candidate = owner
		}
	}
	if candidate == "" {
		return FailoverProposal{}, fmt.Errorf("%w: shard %d", ErrFailoverNoCandidate, shardID)
	}
	if len(healthyOwners) < requiredHealthy {
		return FailoverProposal{}, fmt.Errorf("%w: required=%d healthy=%d", ErrFailoverInsufficientHealthyOwners, requiredHealthy, len(healthyOwners))
	}
	if normalized.FencingToken == ^uint64(0) {
		return FailoverProposal{}, fmt.Errorf("%w: fencing token overflow", ErrFailoverInvalid)
	}

	candidateTopology := Clone(normalized)
	candidateTopology.FencingToken++
	if normalized.Mode == TopologyModeFullReplica {
		candidateTopology.Self = candidate
	} else {
		for index := range candidateTopology.Shards {
			if candidateTopology.Shards[index].ID != shardID {
				continue
			}
			candidateTopology.Shards[index].Primary = candidate
			replicas := make([]string, 0, len(owners)-1)
			for _, owner := range owners {
				if owner != candidate {
					replicas = append(replicas, owner)
				}
			}
			candidateTopology.Shards[index].Replicas = replicas
			break
		}
	}
	candidateTopology, err = Normalize(candidateTopology)
	if err != nil {
		return FailoverProposal{}, fmt.Errorf("%w: candidate topology: %v", ErrFailoverInvalid, err)
	}
	commit, err := NewTopologyCommit(normalized.Fingerprint(), candidateTopology)
	if err != nil {
		return FailoverProposal{}, fmt.Errorf("%w: commit: %v", ErrFailoverInvalid, err)
	}
	return FailoverProposal{
		ShardID:               shardID,
		PreviousPrimary:       shard.Primary,
		CandidatePrimary:      candidate,
		HealthyOwners:         append([]string(nil), healthyOwners...),
		RequiredHealthyOwners: requiredHealthy,
		Commit:                commit,
	}, nil
}

func failoverShard(topology ClusterTopology, shardID uint32) (TopologyShard, bool) {
	if topology.Mode == TopologyModeFullReplica {
		if shardID != 0 {
			return TopologyShard{}, false
		}
		return topology.FullReplicaShard()
	}
	for _, shard := range topology.Shards {
		if shard.ID == shardID {
			return shard, true
		}
	}
	return TopologyShard{}, false
}
