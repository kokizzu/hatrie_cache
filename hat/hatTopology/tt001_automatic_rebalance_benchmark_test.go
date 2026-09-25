package hatTopology

import (
	"fmt"
	"testing"
)

func tt001BenchmarkTopology() ClusterTopology {
	const bucketCount = uint32(4096)
	const shardCount = uint32(32)
	nodes := make([]TopologyNode, 8)
	for index := range nodes {
		nodes[index] = TopologyNode{ID: fmt.Sprintf("node-%02d", index), Address: fmt.Sprintf("%d", index)}
	}
	shards := make([]TopologyShard, shardCount)
	ranges := make([]TopologyBucketRange, shardCount)
	width := bucketCount / shardCount
	for index := range shards {
		shards[index] = TopologyShard{ID: uint32(index + 1), Primary: nodes[0].ID, Replicas: []string{nodes[1].ID}}
		start := uint32(index) * width
		end := start + width - 1
		ranges[index] = TopologyBucketRange{Start: start, End: end, Shard: uint32(index + 1)}
	}
	return ClusterTopology{
		Version:      Version,
		Mode:         TopologyModeSharded,
		BucketCount:  bucketCount,
		BucketRanges: ranges,
		FencingToken: 7,
		Nodes:        nodes,
		Shards:       shards,
	}
}

func BenchmarkTT001BeforeManualMigrationPlan(b *testing.B) {
	current := tt001BenchmarkTopology()
	target, err := PlanAutomaticBucketRebalance(current, AutomaticBucketRebalanceOptions{})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := PlanBucketMigrations(current, target.Target); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTT001AfterAutomaticRebalancePlan(b *testing.B) {
	current := tt001BenchmarkTopology()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := PlanAutomaticBucketRebalance(current, AutomaticBucketRebalanceOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}
