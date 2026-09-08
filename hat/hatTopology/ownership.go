package hatTopology

import (
	"fmt"
	"strings"
)

// PartitionOwnership is an immutable-by-convention snapshot of the nodes that
// own one logical partition and the topology generation that authorizes it.
// It is derived from ClusterTopology and is intentionally separate from the
// topology JSON schema so older nodes can continue to read existing files.
type PartitionOwnership struct {
	ShardID             uint32   `json:"shard_id"`
	Primary             string   `json:"primary"`
	Replicas            []string `json:"replicas,omitempty"`
	TopologyFingerprint string   `json:"topology_fingerprint"`
	FencingToken        uint64   `json:"fencing_token,omitempty"`
}

// OwnershipForShard returns the current ownership snapshot for shardID. Full
// replica topologies expose their single logical partition as shard zero.
func (topology ClusterTopology) OwnershipForShard(shardID uint32) (PartitionOwnership, bool) {
	normalized, err := Normalize(topology)
	if err != nil {
		return PartitionOwnership{}, false
	}
	return partitionOwnershipForNormalizedShard(normalized, shardID)
}

// OwnershipForKey returns the ownership snapshot for the partition selected
// by key.
func (topology ClusterTopology) OwnershipForKey(key string) (PartitionOwnership, bool) {
	normalized, err := Normalize(topology)
	if err != nil {
		return PartitionOwnership{}, false
	}
	route, ok := normalized.RouteForKey(key)
	if !ok {
		return PartitionOwnership{}, false
	}
	return partitionOwnershipForNormalizedShard(normalized, route.Shard.ID)
}

func partitionOwnershipForNormalizedShard(topology ClusterTopology, shardID uint32) (PartitionOwnership, bool) {
	var shard TopologyShard
	if ModeFor(topology) == TopologyModeFullReplica {
		if shardID != 0 {
			return PartitionOwnership{}, false
		}
		var ok bool
		shard, ok = topology.FullReplicaShard()
		if !ok {
			return PartitionOwnership{}, false
		}
	} else {
		var found bool
		for _, candidate := range topology.Shards {
			if candidate.ID == shardID {
				shard, found = candidate, true
				break
			}
		}
		if !found {
			return PartitionOwnership{}, false
		}
	}
	return PartitionOwnership{
		ShardID:             shard.ID,
		Primary:             shard.Primary,
		Replicas:            append([]string(nil), shard.Replicas...),
		TopologyFingerprint: fingerprintNormalized(topology),
		FencingToken:        topology.FencingToken,
	}, true
}

// Owners returns primary first, followed by replicas, as an independent
// slice.
func (ownership PartitionOwnership) Owners() []string {
	owners := make([]string, 0, 1+len(ownership.Replicas))
	if ownership.Primary != "" {
		owners = append(owners, ownership.Primary)
	}
	return append(owners, ownership.Replicas...)
}

// IsPrimary reports whether nodeID is the write owner for the partition.
func (ownership PartitionOwnership) IsPrimary(nodeID string) bool {
	return strings.TrimSpace(nodeID) != "" && ownership.Primary == strings.TrimSpace(nodeID)
}

// IsOwner reports whether nodeID is the primary or one of the replicas.
func (ownership PartitionOwnership) IsOwner(nodeID string) bool {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return false
	}
	if ownership.Primary == nodeID {
		return true
	}
	for _, replica := range ownership.Replicas {
		if replica == nodeID {
			return true
		}
	}
	return false
}

// ValidatePartitionOwnership verifies that a snapshot still describes the
// current partition and topology generation.
func (topology ClusterTopology) ValidatePartitionOwnership(ownership PartitionOwnership) error {
	normalized, err := Normalize(topology)
	if err != nil {
		return err
	}
	expected, ok := partitionOwnershipForNormalizedShard(normalized, ownership.ShardID)
	if !ok {
		return fmt.Errorf("hatriecache: partition %d is not present in topology", ownership.ShardID)
	}
	if ownership.Primary != expected.Primary {
		return fmt.Errorf("hatriecache: partition %d primary changed from %q to %q", ownership.ShardID, ownership.Primary, expected.Primary)
	}
	if len(ownership.Replicas) != len(expected.Replicas) {
		return fmt.Errorf("hatriecache: partition %d replica set changed", ownership.ShardID)
	}
	for index := range expected.Replicas {
		if ownership.Replicas[index] != expected.Replicas[index] {
			return fmt.Errorf("hatriecache: partition %d replica set changed", ownership.ShardID)
		}
	}
	if ownership.TopologyFingerprint != expected.TopologyFingerprint {
		return fmt.Errorf("hatriecache: partition %d topology fingerprint is stale", ownership.ShardID)
	}
	if ownership.FencingToken != expected.FencingToken {
		return fmt.Errorf("hatriecache: partition %d fencing token is stale", ownership.ShardID)
	}
	return nil
}

// ValidatePartitionWrite verifies that nodeID is the current primary and that
// the supplied fencing token belongs to the same ownership snapshot.
func (topology ClusterTopology) ValidatePartitionWrite(ownership PartitionOwnership, nodeID string, fencingToken uint64) error {
	if err := topology.ValidatePartitionOwnership(ownership); err != nil {
		return err
	}
	if !ownership.IsPrimary(nodeID) {
		return fmt.Errorf("hatriecache: node %q is not the primary for partition %d", strings.TrimSpace(nodeID), ownership.ShardID)
	}
	if fencingToken != ownership.FencingToken {
		return fmt.Errorf("hatriecache: partition %d fencing token %d does not match current token %d", ownership.ShardID, fencingToken, ownership.FencingToken)
	}
	return nil
}
