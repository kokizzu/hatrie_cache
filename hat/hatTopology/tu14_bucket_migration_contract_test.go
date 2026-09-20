package hatTopology

import (
	"errors"
	"testing"
)

func TestTU14RejectsWhitespacePaddedMigrationIDs(t *testing.T) {
	plan := BucketMigrationPlan{
		ID:     " bucket-1 ",
		Kind:   BucketMigrationMove,
		Bucket: 1,
		SourceOwnership: PartitionOwnership{
			ShardID:             1,
			Primary:             "node-a",
			TopologyFingerprint: "source-fingerprint",
		},
		TargetOwnership: PartitionOwnership{
			ShardID:             2,
			Primary:             "node-b",
			TopologyFingerprint: "target-fingerprint",
		},
	}

	if err := validateBucketMigrationPlan(plan); !errors.Is(err, ErrBucketMigrationPlanInvalid) {
		t.Fatalf("expected whitespace-padded ID to be rejected, got %v", err)
	}
}

func TestTU14RequiresExactCatchUpSequence(t *testing.T) {
	coordinator, err := NewBucketMigrationCoordinator(BucketMigrationCoordinatorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	plan := BucketMigrationPlan{
		ID:     "bucket-1",
		Kind:   BucketMigrationMove,
		Bucket: 1,
		SourceOwnership: PartitionOwnership{
			ShardID:             1,
			Primary:             "node-a",
			TopologyFingerprint: "source-fingerprint",
		},
		TargetOwnership: PartitionOwnership{
			ShardID:             2,
			Primary:             "node-b",
			TopologyFingerprint: "target-fingerprint",
		},
	}
	if _, err := coordinator.Start(plan); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.BeginCopy(plan.ID); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.MarkCaughtUp(plan.ID, 7, 8); !errors.Is(err, ErrBucketMigrationNotCaughtUp) {
		t.Fatalf("expected ahead target sequence to be rejected, got %v", err)
	}
}

func TestTU14PlansBackupOnlyOwnershipChanges(t *testing.T) {
	current := benchmarkTU14CurrentTopology()
	target := benchmarkTU14CurrentTopology()
	target.FencingToken = 2
	target.Shards[0].Replicas = []string{"node-c"}

	plans, err := PlanBucketMigrations(current, target)
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) != 2 {
		t.Fatalf("expected two bucket plans for shard 1 replica change, got %d", len(plans))
	}
	for _, plan := range plans {
		if plan.Kind != BucketMigrationBackup {
			t.Fatalf("expected backup plan for bucket %d, got %s", plan.Bucket, plan.Kind)
		}
		if plan.SourceOwnership.ShardID != plan.TargetOwnership.ShardID {
			t.Fatalf("backup plan moved shard for bucket %d: %#v", plan.Bucket, plan)
		}
	}
}
