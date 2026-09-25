package hatTopology

import (
	"reflect"
	"testing"
)

func TestTT001AutomaticBucketRebalanceBalancesShardPrimaries(t *testing.T) {
	current := ClusterTopology{
		Version:     Version,
		Mode:        TopologyModeSharded,
		BucketCount: 8,
		BucketRanges: []TopologyBucketRange{
			{Start: 0, End: 1, Shard: 1},
			{Start: 2, End: 3, Shard: 2},
			{Start: 4, End: 5, Shard: 3},
			{Start: 6, End: 7, Shard: 4},
		},
		FencingToken: 7,
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "a"},
			{ID: "node-b", Address: "b"},
			{ID: "node-c", Address: "c"},
			{ID: "node-d", Address: "d"},
		},
		Shards: []TopologyShard{
			{ID: 1, Primary: "node-a", Replicas: []string{"node-b", "node-c"}},
			{ID: 2, Primary: "node-a", Replicas: []string{"node-b", "node-d"}},
			{ID: 3, Primary: "node-a", Replicas: []string{"node-c", "node-d"}},
			{ID: 4, Primary: "node-a", Replicas: []string{"node-b", "node-c"}},
		},
	}
	original := Clone(current)

	plan, err := PlanAutomaticBucketRebalance(current, AutomaticBucketRebalanceOptions{})
	if err != nil {
		t.Fatalf("PlanAutomaticBucketRebalance() error = %v", err)
	}
	if !reflect.DeepEqual(current, original) {
		t.Fatalf("planner mutated current topology: %#v", current)
	}
	if got, want := plan.BeforePrimaryBuckets["node-a"], uint32(8); got != want {
		t.Fatalf("before node-a buckets = %d, want %d", got, want)
	}
	for _, node := range []string{"node-a", "node-b", "node-c", "node-d"} {
		if got, want := plan.AfterPrimaryBuckets[node], uint32(2); got != want {
			t.Fatalf("after %s buckets = %d, want %d", node, got, want)
		}
	}
	if got, want := plan.Target.FencingToken, current.FencingToken+1; got != want {
		t.Fatalf("target fencing token = %d, want %d", got, want)
	}
	if got, want := len(plan.Migrations), 6; got != want {
		t.Fatalf("migration count = %d, want %d", got, want)
	}
	for _, shard := range plan.Target.Shards {
		if len(shard.Replicas) != 2 {
			t.Fatalf("shard %d replica count = %d, want 2", shard.ID, len(shard.Replicas))
		}
		for _, replica := range shard.Replicas {
			if replica == shard.Primary {
				t.Fatalf("shard %d primary %q also appears in replicas %#v", shard.ID, shard.Primary, shard.Replicas)
			}
		}
	}
}

func TestTT001AutomaticBucketRebalanceHonorsMoveAndMaintenanceLimits(t *testing.T) {
	current := ClusterTopology{
		Version:     Version,
		Mode:        TopologyModeSharded,
		BucketCount: 4,
		BucketRanges: []TopologyBucketRange{
			{Start: 0, End: 1, Shard: 1},
			{Start: 2, End: 3, Shard: 2},
		},
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "a", Maintenance: true},
			{ID: "node-b", Address: "b"},
			{ID: "node-c", Address: "c"},
		},
		Shards: []TopologyShard{
			{ID: 1, Primary: "node-a", Replicas: []string{"node-b"}},
			{ID: 2, Primary: "node-b", Replicas: []string{"node-c"}},
		},
	}
	plan, err := PlanAutomaticBucketRebalance(current, AutomaticBucketRebalanceOptions{MaxPrimaryMoves: 1})
	if err != nil {
		t.Fatalf("PlanAutomaticBucketRebalance() error = %v", err)
	}
	if got, want := plan.PrimaryMoves, 1; got != want {
		t.Fatalf("primary moves = %d, want %d", got, want)
	}
	for _, shard := range plan.Target.Shards {
		if shard.Primary == "node-a" {
			t.Fatalf("maintenance node remains primary on shard %d", shard.ID)
		}
	}
}

func TestTT001AutomaticBucketRebalanceRejectsInvalidTopology(t *testing.T) {
	if _, err := PlanAutomaticBucketRebalance(ClusterTopology{Mode: TopologyModeFullReplica}, AutomaticBucketRebalanceOptions{}); err == nil {
		t.Fatal("full-replica topology accepted")
	}
	if _, err := PlanAutomaticBucketRebalance(ClusterTopology{}, AutomaticBucketRebalanceOptions{MaxPrimaryMoves: -1}); err == nil {
		t.Fatal("negative move limit accepted")
	}
}

func TestTT001AutomaticBucketRebalanceDoesNotAdvanceFencingForNoop(t *testing.T) {
	current := ClusterTopology{
		Version:     Version,
		Mode:        TopologyModeSharded,
		BucketCount: 2,
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "a"},
			{ID: "node-b", Address: "b"},
		},
		Shards: []TopologyShard{
			{ID: 1, Primary: "node-a", Replicas: []string{"node-b"}},
			{ID: 2, Primary: "node-b", Replicas: []string{"node-a"}},
		},
		FencingToken: 9,
	}
	plan, err := PlanAutomaticBucketRebalance(current, AutomaticBucketRebalanceOptions{})
	if err != nil {
		t.Fatalf("PlanAutomaticBucketRebalance() error = %v", err)
	}
	if plan.PrimaryMoves != 0 || len(plan.Migrations) != 0 {
		t.Fatalf("noop plan = %#v, want no moves", plan)
	}
	if plan.Target.FencingToken != current.FencingToken {
		t.Fatalf("noop target fencing token = %d, want %d", plan.Target.FencingToken, current.FencingToken)
	}
}
