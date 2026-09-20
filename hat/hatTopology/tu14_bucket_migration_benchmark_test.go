package hatTopology

import "testing"

func benchmarkTU14Topology(fencingToken uint64, ranges []TopologyBucketRange) ClusterTopology {
	return ClusterTopology{
		Version:      Version,
		Mode:         TopologyModeSharded,
		BucketCount:  4,
		BucketRanges: append([]TopologyBucketRange(nil), ranges...),
		FencingToken: fencingToken,
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "127.0.0.1:7101"},
			{ID: "node-b", Address: "127.0.0.1:7102"},
			{ID: "node-c", Address: "127.0.0.1:7103"},
			{ID: "node-d", Address: "127.0.0.1:7104"},
		},
		Shards: []TopologyShard{
			{ID: 1, Primary: "node-a", Replicas: []string{"node-b"}},
			{ID: 2, Primary: "node-c", Replicas: []string{"node-d"}},
		},
	}
}

func benchmarkTU14CurrentTopology() ClusterTopology {
	return benchmarkTU14Topology(1, []TopologyBucketRange{
		{Start: 0, End: 1, Shard: 1},
		{Start: 2, End: 3, Shard: 2},
	})
}

func benchmarkTU14TargetTopology() ClusterTopology {
	return benchmarkTU14Topology(2, []TopologyBucketRange{
		{Start: 0, End: 0, Shard: 1},
		{Start: 1, End: 3, Shard: 2},
	})
}

// benchmarkTU14ManualBucketDiff models the pre-coordinator caller-side work:
// normalize both snapshots, compare owners, and count changed buckets.
func benchmarkTU14ManualBucketDiff(current, target ClusterTopology) int {
	current, currentErr := Normalize(current)
	target, targetErr := Normalize(target)
	if currentErr != nil || targetErr != nil || current.BucketCount != target.BucketCount {
		return -1
	}
	changed := 0
	for bucket := uint32(0); bucket < target.BucketCount; bucket++ {
		currentShard, currentOK := current.shardForBucket(bucket, current.Shards)
		targetShard, targetOK := target.shardForBucket(bucket, target.Shards)
		if !currentOK || !targetOK || currentShard.ID != targetShard.ID || currentShard.Primary != targetShard.Primary {
			changed++
		}
	}
	return changed
}

func BenchmarkTU14BeforeManualBucketDiff(b *testing.B) {
	current := benchmarkTU14CurrentTopology()
	target := benchmarkTU14TargetTopology()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if benchmarkTU14ManualBucketDiff(current, target) != 1 {
			b.Fatal("manual diff returned the wrong bucket count")
		}
	}
}

func BenchmarkTU14AfterPlanBucketMigrations(b *testing.B) {
	current := benchmarkTU14CurrentTopology()
	target := benchmarkTU14TargetTopology()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		plans, err := PlanBucketMigrations(current, target)
		if err != nil || len(plans) != 1 {
			b.Fatalf("planner returned %d plans with error %v", len(plans), err)
		}
	}
}

func BenchmarkTU14AfterMigrationLifecycle(b *testing.B) {
	current := benchmarkTU14CurrentTopology()
	target := benchmarkTU14TargetTopology()
	plans, err := PlanBucketMigrations(current, target)
	if err != nil || len(plans) != 1 {
		b.Fatalf("planner returned %d plans with error %v", len(plans), err)
	}
	plan := plans[0]
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		coordinator, err := NewBucketMigrationCoordinator(BucketMigrationCoordinatorOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := coordinator.Start(plan); err != nil {
			b.Fatal(err)
		}
		if err := coordinator.BeginCopy(plan.ID); err != nil {
			b.Fatal(err)
		}
		if err := coordinator.RecordCopy(plan.ID, 10, 1024); err != nil {
			b.Fatal(err)
		}
		if err := coordinator.MarkCaughtUp(plan.ID, 10, 10); err != nil {
			b.Fatal(err)
		}
		if err := coordinator.MarkCutoverReady(plan.ID); err != nil {
			b.Fatal(err)
		}
		if err := coordinator.CompleteCutover(plan.ID, target); err != nil {
			b.Fatal(err)
		}
	}
}
