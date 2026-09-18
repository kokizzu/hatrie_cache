package hatPipeline

import (
	"errors"
	"testing"
)

func TestQueuePartitionOwnershipMigrationCutover(t *testing.T) {
	ownership, err := NewQueuePartitionOwnership(QueuePartitionOwnershipOptions{
		PartitionCount: 4,
		InitialOwners:  []string{"node-a", "node-a", "node-b", "node-b"},
	})
	if err != nil {
		t.Fatalf("NewQueuePartitionOwnership() error = %v", err)
	}

	initial, err := ownership.Assignment(1)
	if err != nil {
		t.Fatalf("Assignment() error = %v", err)
	}
	if initial.Owner != "node-a" || initial.State != QueuePartitionOwnershipStable || initial.Generation != 1 {
		t.Fatalf("initial assignment = %+v", initial)
	}

	migration, err := ownership.BeginMigration(1, "node-b", 100)
	if err != nil {
		t.Fatalf("BeginMigration() error = %v", err)
	}
	if migration.Source != "node-a" || migration.Target != "node-b" || migration.Fence != 100 {
		t.Fatalf("migration = %+v", migration)
	}

	moving, err := ownership.Assignment(1)
	if err != nil {
		t.Fatalf("moving Assignment() error = %v", err)
	}
	if moving.Owner != "node-a" || moving.Target != "node-b" || moving.State != QueuePartitionOwnershipMigrating || moving.MigrationReady {
		t.Fatalf("moving assignment = %+v", moving)
	}
	if _, err := ownership.AcknowledgeCatchUp(migration, 99); !errors.Is(err, ErrQueuePartitionMigrationNotCaughtUp) {
		t.Fatalf("lagging acknowledgement error = %v, want %v", err, ErrQueuePartitionMigrationNotCaughtUp)
	}

	ready, err := ownership.AcknowledgeCatchUp(migration, 100)
	if err != nil {
		t.Fatalf("AcknowledgeCatchUp() error = %v", err)
	}
	if !ready.MigrationReady || ready.Owner != "node-a" || ready.Target != "node-b" {
		t.Fatalf("ready assignment = %+v", ready)
	}

	cutover, err := ownership.Cutover(migration)
	if err != nil {
		t.Fatalf("Cutover() error = %v", err)
	}
	if cutover.Owner != "node-b" || cutover.Target != "" || cutover.State != QueuePartitionOwnershipStable || cutover.Generation != 2 {
		t.Fatalf("cutover assignment = %+v", cutover)
	}
	if _, err := ownership.Cutover(migration); !errors.Is(err, ErrQueuePartitionMigrationNotActive) {
		t.Fatalf("reused migration error = %v, want %v", err, ErrQueuePartitionMigrationNotActive)
	}
}

func TestQueuePartitionOwnershipAbortInvalidatesMigration(t *testing.T) {
	ownership, err := NewQueuePartitionOwnership(QueuePartitionOwnershipOptions{
		PartitionCount: 2,
		InitialOwners:  []string{"node-a", "node-b"},
	})
	if err != nil {
		t.Fatalf("NewQueuePartitionOwnership() error = %v", err)
	}
	migration, err := ownership.BeginMigration(0, "node-b", 0)
	if err != nil {
		t.Fatalf("BeginMigration() error = %v", err)
	}
	aborted, err := ownership.AbortMigration(migration)
	if err != nil {
		t.Fatalf("AbortMigration() error = %v", err)
	}
	if aborted.Owner != "node-a" || aborted.Target != "" || aborted.State != QueuePartitionOwnershipStable || aborted.Generation != 2 {
		t.Fatalf("aborted assignment = %+v", aborted)
	}
	if _, err := ownership.AcknowledgeCatchUp(migration, 0); !errors.Is(err, ErrQueuePartitionMigrationNotActive) {
		t.Fatalf("aborted acknowledgement error = %v, want %v", err, ErrQueuePartitionMigrationNotActive)
	}
}

func TestQueuePartitionOwnershipRejectsInvalidOptionsAndRoutes(t *testing.T) {
	for _, options := range []QueuePartitionOwnershipOptions{
		{PartitionCount: 0},
		{PartitionCount: -1},
		{PartitionCount: 2, InitialOwners: []string{"only-one"}},
		{PartitionCount: MaxQueuePartitionOwnershipPartitions + 1},
	} {
		if _, err := NewQueuePartitionOwnership(options); !errors.Is(err, ErrQueuePartitionOwnershipOptionsInvalid) {
			t.Fatalf("options %+v error = %v, want %v", options, err, ErrQueuePartitionOwnershipOptionsInvalid)
		}
	}

	ownership, err := NewQueuePartitionOwnership(QueuePartitionOwnershipOptions{PartitionCount: 2})
	if err != nil {
		t.Fatalf("unassigned manager error = %v", err)
	}
	if _, err := ownership.Owner(0); !errors.Is(err, ErrQueuePartitionUnassigned) {
		t.Fatalf("unassigned Owner() error = %v, want %v", err, ErrQueuePartitionUnassigned)
	}
	if _, err := ownership.Assignment(-1); !errors.Is(err, ErrQueuePartitionInvalid) {
		t.Fatalf("negative Assignment() error = %v, want %v", err, ErrQueuePartitionInvalid)
	}
	if _, err := ownership.BeginMigration(0, "", 1); !errors.Is(err, ErrQueuePartitionMigrationTargetRequired) {
		t.Fatalf("empty target error = %v, want %v", err, ErrQueuePartitionMigrationTargetRequired)
	}

	owned, err := NewQueuePartitionOwnership(QueuePartitionOwnershipOptions{
		PartitionCount: 1,
		InitialOwners:  []string{"node-a"},
	})
	if err != nil {
		t.Fatalf("owned manager error = %v", err)
	}
	if _, err := owned.BeginMigration(0, "node-a", 1); !errors.Is(err, ErrQueuePartitionMigrationSameOwner) {
		t.Fatalf("same owner error = %v, want %v", err, ErrQueuePartitionMigrationSameOwner)
	}
}

func TestQueuePartitionOwnershipAssignmentsAreIndependentCopies(t *testing.T) {
	ownership, err := NewQueuePartitionOwnership(QueuePartitionOwnershipOptions{
		PartitionCount: 2,
		InitialOwners:  []string{"node-a", "node-b"},
	})
	if err != nil {
		t.Fatalf("NewQueuePartitionOwnership() error = %v", err)
	}
	assignments := ownership.Assignments()
	if len(assignments) != 2 {
		t.Fatalf("Assignments() length = %d, want 2", len(assignments))
	}
	assignments[0].Owner = "mutated"
	assignmentsAgain := ownership.Assignments()
	if assignmentsAgain[0].Owner != "node-a" {
		t.Fatalf("manager owner changed through snapshot copy = %q", assignmentsAgain[0].Owner)
	}
}

func TestQueuePartitionOwnershipSnapshotStaysStableAcrossCutover(t *testing.T) {
	ownership, err := NewQueuePartitionOwnership(QueuePartitionOwnershipOptions{
		PartitionCount: 1,
		InitialOwners:  []string{"node-a"},
	})
	if err != nil {
		t.Fatalf("NewQueuePartitionOwnership() error = %v", err)
	}
	view := ownership.Snapshot()
	migration, err := ownership.BeginMigration(0, "node-b", 4)
	if err != nil {
		t.Fatalf("BeginMigration() error = %v", err)
	}
	if _, err := ownership.AcknowledgeCatchUp(migration, 4); err != nil {
		t.Fatalf("AcknowledgeCatchUp() error = %v", err)
	}
	if _, err := ownership.Cutover(migration); err != nil {
		t.Fatalf("Cutover() error = %v", err)
	}
	if owner, err := view.Owner(0); err != nil || owner != "node-a" {
		t.Fatalf("stale view owner = %q, %v; want node-a", owner, err)
	}
	latest := ownership.Snapshot()
	if owner, err := latest.Owner(0); err != nil || owner != "node-b" {
		t.Fatalf("latest view owner = %q, %v; want node-b", owner, err)
	}
	if owner := view.OwnerUnchecked(0); owner != "node-a" {
		t.Fatalf("unchecked stale view owner = %q, want node-a", owner)
	}
}
