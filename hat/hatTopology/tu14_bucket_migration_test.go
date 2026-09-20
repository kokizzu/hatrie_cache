package hatTopology

import (
	"errors"
	"reflect"
	"testing"
)

func TestTU14PlansChangedBucketsWithBackupOwnership(t *testing.T) {
	current := tu14Topology([]TopologyBucketRange{{Start: 0, End: 1, Shard: 1}, {Start: 2, End: 3, Shard: 2}}, 1)
	target := tu14Topology([]TopologyBucketRange{{Start: 0, End: 0, Shard: 1}, {Start: 1, End: 3, Shard: 2}}, 2)

	plans, err := PlanBucketMigrations(current, target)
	if err != nil {
		t.Fatalf("PlanBucketMigrations() error = %v", err)
	}
	if len(plans) != 1 || plans[0].Bucket != 1 {
		t.Fatalf("plans = %#v, want one plan for bucket 1", plans)
	}
	plan := plans[0]
	if plan.Kind != BucketMigrationMove || plan.SourceOwnership.Primary != "node-a" || plan.TargetOwnership.Primary != "node-c" {
		t.Fatalf("plan ownership = %#v, want node-a -> node-c move", plan)
	}
	if !reflect.DeepEqual(plan.TargetOwnership.Replicas, []string{"node-d"}) {
		t.Fatalf("target backups = %#v, want node-d", plan.TargetOwnership.Replicas)
	}
	repeated, err := PlanBucketMigrations(current, target)
	if err != nil || !reflect.DeepEqual(plans, repeated) {
		t.Fatalf("repeated plans = %#v/%v, want deterministic %#v", repeated, err, plans)
	}
}

func TestTU14MigrationLifecycleFencesCutoverAndResumes(t *testing.T) {
	current := tu14Topology([]TopologyBucketRange{{Start: 0, End: 1, Shard: 1}, {Start: 2, End: 3, Shard: 2}}, 1)
	target := tu14Topology([]TopologyBucketRange{{Start: 0, End: 0, Shard: 1}, {Start: 1, End: 3, Shard: 2}}, 2)
	plans, err := PlanBucketMigrations(current, target)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := NewBucketMigrationCoordinator(BucketMigrationCoordinatorOptions{MaxMigrations: 2})
	if err != nil {
		t.Fatal(err)
	}
	status, err := coordinator.Start(plans[0])
	if err != nil || status.Phase != BucketMigrationPlanned {
		t.Fatalf("Start() = %#v/%v, want planned", status, err)
	}
	if _, err := coordinator.Start(plans[0]); err != nil {
		t.Fatalf("idempotent Start() error = %v", err)
	}
	if err := coordinator.BeginCopy(plans[0].ID); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.RecordCopy(plans[0].ID, 10, 1000); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.RecordCopy(plans[0].ID, ^uint64(0), 0); !errors.Is(err, ErrBucketMigrationProgressInvalid) {
		t.Fatalf("overflow RecordCopy() error = %v, want progress error", err)
	}
	if err := coordinator.MarkCaughtUp(plans[0].ID, 7, 6); !errors.Is(err, ErrBucketMigrationNotCaughtUp) {
		t.Fatalf("lagging MarkCaughtUp() error = %v, want not caught up", err)
	}
	if err := coordinator.MarkCaughtUp(plans[0].ID, 7, 7); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.MarkCutoverReady(plans[0].ID); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.CompleteCutover(plans[0].ID, current); !errors.Is(err, ErrBucketMigrationTopologyMismatch) {
		t.Fatalf("stale CompleteCutover() error = %v, want topology mismatch", err)
	}
	if err := coordinator.CompleteCutover(plans[0].ID, target); err != nil {
		t.Fatal(err)
	}
	status, err = coordinator.Status(plans[0].ID)
	if err != nil || status.Phase != BucketMigrationCompleted || status.CopiedRecords != 10 || status.CopiedBytes != 1000 {
		t.Fatalf("completed status = %#v/%v", status, err)
	}

	secondPlan := plans[0]
	secondPlan.ID += "-resume"
	if _, err := coordinator.Start(secondPlan); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.BeginCopy(secondPlan.ID); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Abort(secondPlan.ID); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Resume(secondPlan.ID); err != nil {
		t.Fatal(err)
	}
	resumed, err := coordinator.Status(secondPlan.ID)
	if err != nil || resumed.Phase != BucketMigrationCopying {
		t.Fatalf("resumed status = %#v/%v, want copying", resumed, err)
	}
}

func tu14Topology(ranges []TopologyBucketRange, fencing uint64) ClusterTopology {
	return ClusterTopology{
		Version:      Version,
		Mode:         TopologyModeSharded,
		BucketCount:  4,
		BucketRanges: ranges,
		FencingToken: fencing,
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "a"},
			{ID: "node-b", Address: "b"},
			{ID: "node-c", Address: "c"},
			{ID: "node-d", Address: "d"},
		},
		Shards: []TopologyShard{
			{ID: 1, Primary: "node-a", Replicas: []string{"node-b"}},
			{ID: 2, Primary: "node-c", Replicas: []string{"node-d"}},
		},
	}
}
