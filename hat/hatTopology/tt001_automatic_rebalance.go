package hatTopology

import (
	"errors"
	"fmt"
	"sort"
)

// ErrAutomaticBucketRebalanceInvalid reports an invalid input or a rebalance
// that cannot preserve the current replica contract.
var ErrAutomaticBucketRebalanceInvalid = errors.New("hatriecache: invalid automatic bucket rebalance")

// AutomaticBucketRebalanceOptions controls target-primary generation. A zero
// MaxPrimaryMoves permits every beneficial primary move. Maintenance nodes are
// excluded unless IncludeMaintenance is set.
type AutomaticBucketRebalanceOptions struct {
	MaxPrimaryMoves    int
	IncludeMaintenance bool
}

// AutomaticBucketRebalancePlan is a deterministic target topology plus the
// existing caller-executable bucket migration plans. Planning does not copy
// data or publish ownership; callers run Migrations through their transfer
// transport and coordinator before committing Target.
type AutomaticBucketRebalancePlan struct {
	Target               ClusterTopology
	Migrations           []BucketMigrationPlan
	BeforePrimaryBuckets map[string]uint32
	AfterPrimaryBuckets  map[string]uint32
	PrimaryMoves         int
}

type automaticRebalanceShard struct {
	shard  TopologyShard
	weight uint32
}

// PlanAutomaticBucketRebalance derives a balanced primary assignment from the
// current bucket distribution. Virtual shards remain stable, so existing
// BucketMigrationCoordinator transfer and fencing semantics continue to apply.
func PlanAutomaticBucketRebalance(current ClusterTopology, options AutomaticBucketRebalanceOptions) (AutomaticBucketRebalancePlan, error) {
	if options.MaxPrimaryMoves < 0 {
		return AutomaticBucketRebalancePlan{}, fmt.Errorf("%w: max primary moves must not be negative", ErrAutomaticBucketRebalanceInvalid)
	}
	normalized, err := Normalize(current)
	if err != nil {
		return AutomaticBucketRebalancePlan{}, fmt.Errorf("%w: current topology: %v", ErrAutomaticBucketRebalanceInvalid, err)
	}
	if normalized.Mode != TopologyModeSharded || normalized.BucketCount == 0 || len(normalized.Shards) == 0 {
		return AutomaticBucketRebalancePlan{}, fmt.Errorf("%w: topology must be sharded with buckets", ErrAutomaticBucketRebalanceInvalid)
	}
	nodes := eligibleRebalanceNodes(normalized.Nodes, options.IncludeMaintenance)
	if len(nodes) == 0 {
		return AutomaticBucketRebalancePlan{}, fmt.Errorf("%w: no eligible primary nodes", ErrAutomaticBucketRebalanceInvalid)
	}
	if normalized.FencingToken == ^uint64(0) {
		return AutomaticBucketRebalancePlan{}, fmt.Errorf("%w: fencing token overflow", ErrAutomaticBucketRebalanceInvalid)
	}

	weights := make(map[uint32]uint32, len(normalized.Shards))
	for bucket := uint32(0); bucket < normalized.BucketCount; bucket++ {
		shard, ok := normalized.shardForBucket(bucket, normalized.Shards)
		if !ok {
			return AutomaticBucketRebalancePlan{}, fmt.Errorf("%w: bucket %d has no shard", ErrAutomaticBucketRebalanceInvalid, bucket)
		}
		weights[shard.ID]++
	}

	before := rebalancePrimaryBucketLoads(normalized, weights)
	desired := automaticRebalancePrimaries(normalized.Shards, weights, nodes)
	target := Clone(normalized)
	target.FencingToken++
	actual := make(map[string]uint32, len(normalized.Nodes))
	for _, node := range normalized.Nodes {
		actual[node.ID] = 0
	}
	primaryMoves := 0
	shards := make(map[uint32]TopologyShard, len(normalized.Shards))
	for _, shard := range normalized.Shards {
		primary := desired[shard.ID]
		if primary == "" {
			return AutomaticBucketRebalancePlan{}, fmt.Errorf("%w: shard %d has no target primary", ErrAutomaticBucketRebalanceInvalid, shard.ID)
		}
		if primary != shard.Primary {
			if options.MaxPrimaryMoves > 0 && primaryMoves >= options.MaxPrimaryMoves {
				if !rebalanceNodeAllowed(shard.Primary, nodes) {
					return AutomaticBucketRebalancePlan{}, fmt.Errorf("%w: move limit prevents evacuating primary %q", ErrAutomaticBucketRebalanceInvalid, shard.Primary)
				}
				primary = shard.Primary
			} else {
				primaryMoves++
			}
		}
		updated := shard
		updated.Primary = primary
		updated.Replicas = automaticRebalanceReplicas(shard, primary, normalized.Nodes, options.IncludeMaintenance)
		if len(updated.Replicas) != len(shard.Replicas) {
			return AutomaticBucketRebalancePlan{}, fmt.Errorf("%w: shard %d cannot preserve %d replicas", ErrAutomaticBucketRebalanceInvalid, shard.ID, len(shard.Replicas))
		}
		shards[updated.ID] = updated
		actual[primary] += weights[shard.ID]
	}
	for index := range target.Shards {
		target.Shards[index] = shards[target.Shards[index].ID]
	}
	if primaryMoves == 0 {
		target.FencingToken = normalized.FencingToken
	}
	migrations, err := PlanBucketMigrations(normalized, target)
	if err != nil {
		return AutomaticBucketRebalancePlan{}, fmt.Errorf("%w: migration plan: %v", ErrAutomaticBucketRebalanceInvalid, err)
	}
	return AutomaticBucketRebalancePlan{
		Target:               target,
		Migrations:           migrations,
		BeforePrimaryBuckets: before,
		AfterPrimaryBuckets:  actual,
		PrimaryMoves:         primaryMoves,
	}, nil
}

func eligibleRebalanceNodes(nodes []TopologyNode, includeMaintenance bool) []TopologyNode {
	eligible := make([]TopologyNode, 0, len(nodes))
	for _, node := range nodes {
		if !includeMaintenance && node.Maintenance {
			continue
		}
		eligible = append(eligible, node)
	}
	sort.Slice(eligible, func(left, right int) bool { return eligible[left].ID < eligible[right].ID })
	return eligible
}

func rebalanceNodeAllowed(nodeID string, nodes []TopologyNode) bool {
	for _, node := range nodes {
		if node.ID == nodeID {
			return true
		}
	}
	return false
}

func rebalancePrimaryBucketLoads(topology ClusterTopology, weights map[uint32]uint32) map[string]uint32 {
	loads := make(map[string]uint32, len(topology.Nodes))
	for _, node := range topology.Nodes {
		loads[node.ID] = 0
	}
	for _, shard := range topology.Shards {
		loads[shard.Primary] += weights[shard.ID]
	}
	return loads
}

func automaticRebalancePrimaries(shards []TopologyShard, weights map[uint32]uint32, nodes []TopologyNode) map[uint32]string {
	loads := make(map[string]uint32, len(nodes))
	for _, node := range nodes {
		loads[node.ID] = 0
	}
	for _, shard := range shards {
		if _, exists := loads[shard.Primary]; exists {
			loads[shard.Primary] += weights[shard.ID]
		}
	}
	ordered := append([]TopologyShard(nil), shards...)
	sort.Slice(ordered, func(left, right int) bool {
		leftWeight, rightWeight := weights[ordered[left].ID], weights[ordered[right].ID]
		if leftWeight != rightWeight {
			return leftWeight > rightWeight
		}
		return ordered[left].ID < ordered[right].ID
	})
	desired := make(map[uint32]string, len(shards))
	for _, shard := range ordered {
		if current, exists := loads[shard.Primary]; exists {
			loads[shard.Primary] = current - weights[shard.ID]
		}
		best := nodes[0].ID
		bestLoad := loads[best] + weights[shard.ID]
		for _, node := range nodes[1:] {
			candidateLoad := loads[node.ID] + weights[shard.ID]
			if candidateLoad < bestLoad || (candidateLoad == bestLoad && node.ID < best) {
				best = node.ID
				bestLoad = candidateLoad
			}
		}
		desired[shard.ID] = best
		loads[best] += weights[shard.ID]
	}
	return desired
}

func automaticRebalanceReplicas(shard TopologyShard, primary string, nodes []TopologyNode, includeMaintenance bool) []string {
	want := len(shard.Replicas)
	if want == 0 {
		return nil
	}
	if primary == shard.Primary {
		return append([]string(nil), shard.Replicas...)
	}
	allowed := make(map[string]TopologyNode, len(nodes))
	for _, node := range nodes {
		allowed[node.ID] = node
	}
	for _, node := range nodes {
		if _, exists := allowed[node.ID]; !exists {
			allowed[node.ID] = node
		}
	}
	result := make([]string, 0, want)
	seen := map[string]struct{}{primary: {}}
	appendReplica := func(nodeID string) {
		if len(result) >= want || nodeID == "" {
			return
		}
		if node, exists := allowed[nodeID]; !exists || (!includeMaintenance && node.Maintenance) {
			return
		}
		if _, exists := seen[nodeID]; exists {
			return
		}
		seen[nodeID] = struct{}{}
		result = append(result, nodeID)
	}
	appendReplica(shard.Primary)
	for _, replica := range shard.Replicas {
		appendReplica(replica)
	}
	for _, node := range nodes {
		appendReplica(node.ID)
	}
	if len(result) == want {
		return result
	}
	// A maintenance replica may still be the only available copy. Keep it as a
	// last resort rather than silently reducing the configured replica count.
	for _, replica := range shard.Replicas {
		if len(result) >= want {
			break
		}
		if _, exists := seen[replica]; exists || replica == primary {
			continue
		}
		seen[replica] = struct{}{}
		result = append(result, replica)
	}
	return result
}
