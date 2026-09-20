package hatTopology

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestTU14VShardMapRoutesAndPlansMoves(t *testing.T) {
	current, err := NewVShardBucketMap(VShardBucketMapOptions{
		BucketCount:       64,
		ReplicationFactor: 2,
		Generation:        1,
		FencingToken:      10,
		Nodes:             []VShardNode{{ID: "node-a"}, {ID: "node-b"}, {ID: "node-c"}},
	})
	if err != nil {
		t.Fatalf("NewVShardBucketMap() error = %v", err)
	}
	route, ok := current.RouteKey("sg:user:42")
	if !ok || route.Primary == "" || route.Bucket >= 64 || route.ReplicaCount() != 1 {
		t.Fatalf("RouteKey() = %#v/%v", route, ok)
	}
	if got := current.BucketCount(); got != 64 {
		t.Fatalf("BucketCount() = %d, want 64", got)
	}

	plan, err := current.PlanRebalance([]VShardNode{{ID: "node-a"}, {ID: "node-b"}, {ID: "node-d"}}, 2, 11)
	if err != nil {
		t.Fatalf("PlanRebalance() error = %v", err)
	}
	if plan.MoveCount() == 0 || plan.Target().Generation() != 2 || plan.Target().FencingToken() != 11 {
		t.Fatalf("rebalance plan = %#v", plan)
	}
	for _, move := range plan.Moves() {
		if move.Bucket >= 64 || move.Source.Primary == move.Target.Primary && move.Source.ReplicasEqual(move.Target) {
			t.Fatalf("invalid move %#v", move)
		}
	}
}

func TestTU14MigrationRequiresSnapshotThenContiguousWALAndFence(t *testing.T) {
	current, err := NewVShardBucketMap(VShardBucketMapOptions{
		BucketCount:       8,
		ReplicationFactor: 1,
		Generation:        1,
		FencingToken:      20,
		Nodes:             []VShardNode{{ID: "node-a"}, {ID: "node-b"}},
	})
	if err != nil {
		t.Fatalf("NewVShardBucketMap() error = %v", err)
	}
	plan, err := current.PlanRebalance([]VShardNode{{ID: "node-a"}, {ID: "node-c"}}, 2, 21)
	if err != nil || plan.MoveCount() == 0 {
		t.Fatalf("PlanRebalance() = %#v/%v", plan, err)
	}
	move := plan.Moves()[0]
	migration, err := NewVShardMigration(move, VShardMigrationOptions{
		SnapshotSequence: 100,
		WALLastSequence:  102,
	})
	if err != nil {
		t.Fatalf("NewVShardMigration() error = %v", err)
	}
	if err := migration.ApplyWAL(context.Background(), []VShardWALRecord{{Sequence: 101}}, func([]VShardWALRecord) error { return nil }); !errors.Is(err, ErrVShardMigrationPhase) {
		t.Fatalf("ApplyWAL before snapshot error = %v, want phase error", err)
	}
	if err := migration.ApplySnapshot(context.Background(), [32]byte{1}, func() error { return nil }); err != nil {
		t.Fatalf("ApplySnapshot() error = %v", err)
	}
	if err := migration.ApplyWAL(context.Background(), []VShardWALRecord{{Sequence: 102}}, func([]VShardWALRecord) error { return nil }); !errors.Is(err, ErrVShardSequenceGap) {
		t.Fatalf("ApplyWAL gap error = %v, want sequence gap", err)
	}
	if err := migration.ApplyWAL(context.Background(), []VShardWALRecord{{Sequence: 101}, {Sequence: 102}}, func([]VShardWALRecord) error { return nil }); err != nil {
		t.Fatalf("ApplyWAL() error = %v", err)
	}
	if err := migration.Activate(context.Background(), 20, func() error { return nil }); !errors.Is(err, ErrVShardMigrationFenced) {
		t.Fatalf("Activate stale fence error = %v, want fence error", err)
	}
	if err := migration.Activate(context.Background(), 21, func() error { return nil }); err != nil {
		t.Fatalf("Activate() error = %v", err)
	}
}

func TestTU14VShardMapIsDeterministicAndRejectsInvalidConfiguration(t *testing.T) {
	left, err := NewVShardBucketMap(VShardBucketMapOptions{
		BucketCount:       32,
		ReplicationFactor: 2,
		Nodes:             []VShardNode{{ID: "node-c"}, {ID: "node-a"}, {ID: "node-b"}},
	})
	if err != nil {
		t.Fatalf("left map error = %v", err)
	}
	right, err := NewVShardBucketMap(VShardBucketMapOptions{
		BucketCount:       32,
		ReplicationFactor: 2,
		Nodes:             []VShardNode{{ID: "node-a"}, {ID: "node-b"}, {ID: "node-c"}},
	})
	if err != nil {
		t.Fatalf("right map error = %v", err)
	}
	if !reflect.DeepEqual(left.Nodes(), right.Nodes()) {
		t.Fatalf("canonical nodes differ: %#v and %#v", left.Nodes(), right.Nodes())
	}
	for bucket := uint32(0); bucket < left.BucketCount(); bucket++ {
		leftOwnership, leftOK := left.BucketOwnership(bucket)
		rightOwnership, rightOK := right.BucketOwnership(bucket)
		if !leftOK || !rightOK || leftOwnership.Primary != rightOwnership.Primary || !leftOwnership.ReplicasEqual(rightOwnership) {
			t.Fatalf("bucket %d differs: %#v/%v and %#v/%v", bucket, leftOwnership, leftOK, rightOwnership, rightOK)
		}
	}

	invalid := []VShardBucketMapOptions{
		{Nodes: nil},
		{BucketCount: 3, Nodes: []VShardNode{{ID: "node-a"}}},
		{ReplicationFactor: 2, Nodes: []VShardNode{{ID: "node-a"}}},
		{Nodes: []VShardNode{{ID: "node-a"}, {ID: "node-a"}}},
		{Nodes: []VShardNode{{ID: " node-a"}}},
	}
	for index, options := range invalid {
		if _, err := NewVShardBucketMap(options); !errors.Is(err, ErrVShardBucketMapInvalid) && !errors.Is(err, ErrVShardNodeInvalid) {
			t.Errorf("invalid configuration %d error = %v", index, err)
		}
	}
}

func TestTU14MigrationCheckpointRoundTripAndCallbackIsolation(t *testing.T) {
	move := tu14TestMove(t, 1, 2)
	migration, err := NewVShardMigration(move, VShardMigrationOptions{SnapshotSequence: 10, WALLastSequence: 11})
	if err != nil {
		t.Fatalf("NewVShardMigration() error = %v", err)
	}
	if err := migration.ApplySnapshot(context.Background(), [32]byte{7}, func() error { return nil }); err != nil {
		t.Fatalf("ApplySnapshot() error = %v", err)
	}
	payload := []byte("before")
	if err := migration.ApplyWAL(context.Background(), []VShardWALRecord{{Sequence: 11, Payload: payload}}, func(records []VShardWALRecord) error {
		records[0].Payload[0] = 'X'
		return nil
	}); err != nil {
		t.Fatalf("ApplyWAL() error = %v", err)
	}
	if string(payload) != "before" {
		t.Fatalf("WAL callback mutated caller payload: %q", payload)
	}
	checkpoint := migration.Checkpoint()
	encoded, err := checkpoint.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	var decoded VShardMigrationCheckpoint
	if err := decoded.UnmarshalBinary(encoded); err != nil {
		t.Fatalf("UnmarshalBinary() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, checkpoint) {
		t.Fatalf("checkpoint mismatch: %#v and %#v", decoded, checkpoint)
	}
	corrupt := append([]byte(nil), encoded...)
	corrupt[len(corrupt)-1]++
	if err := decoded.UnmarshalBinary(corrupt); !errors.Is(err, ErrVShardMigrationCheckpointCRC) {
		t.Fatalf("corrupt checkpoint error = %v, want checksum error", err)
	}
	resumed, err := NewVShardMigrationFromCheckpoint(move, checkpoint)
	if err != nil {
		t.Fatalf("NewVShardMigrationFromCheckpoint() error = %v", err)
	}
	if err := resumed.Activate(context.Background(), move.Target.FencingToken, func() error { return nil }); err != nil {
		t.Fatalf("resumed Activate() error = %v", err)
	}
	if got := resumed.Status().Phase; got != VShardMigrationActive {
		t.Fatalf("resumed phase = %s, want active", got)
	}
}

func TestTU14MigrationFailureIsTerminalAndCallbacksDoNotHoldLock(t *testing.T) {
	move := tu14TestMove(t, 3, 4)
	migration, err := NewVShardMigration(move, VShardMigrationOptions{SnapshotSequence: 20, WALLastSequence: 20})
	if err != nil {
		t.Fatalf("NewVShardMigration() error = %v", err)
	}
	callbackStarted := make(chan struct{})
	callbackRelease := make(chan struct{})
	callbackDone := make(chan error, 1)
	go func() {
		callbackDone <- migration.ApplySnapshot(context.Background(), [32]byte{9}, func() error {
			close(callbackStarted)
			<-callbackRelease
			return nil
		})
	}()
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("snapshot callback did not start")
	}
	statusDone := make(chan struct{})
	go func() {
		_ = migration.Status()
		close(statusDone)
	}()
	select {
	case <-statusDone:
	case <-time.After(time.Second):
		t.Fatal("Status() blocked during snapshot callback")
	}
	close(callbackRelease)
	if err := <-callbackDone; err != nil {
		t.Fatalf("ApplySnapshot() error = %v", err)
	}

	failing, err := NewVShardMigration(move, VShardMigrationOptions{SnapshotSequence: 20, WALLastSequence: 20})
	if err != nil {
		t.Fatalf("failing migration error = %v", err)
	}
	failure := errors.New("target write failed")
	if err := failing.ApplySnapshot(context.Background(), [32]byte{1}, func() error { return failure }); !errors.Is(err, failure) {
		t.Fatalf("callback failure = %v, want original error", err)
	}
	if got := failing.Status().Phase; got != VShardMigrationFailed {
		t.Fatalf("failed phase = %s, want failed", got)
	}
	if err := failing.Activate(context.Background(), 4, func() error { return nil }); !errors.Is(err, ErrVShardMigrationPhase) {
		t.Fatalf("activation after failure = %v, want phase error", err)
	}
}

func tu14TestMove(t *testing.T, generation, fencingToken uint64) VShardBucketMove {
	t.Helper()
	current, err := NewVShardBucketMap(VShardBucketMapOptions{
		BucketCount:       8,
		ReplicationFactor: 1,
		Generation:        generation,
		FencingToken:      fencingToken,
		Nodes:             []VShardNode{{ID: "node-a"}, {ID: "node-b"}},
	})
	if err != nil {
		t.Fatalf("current map error = %v", err)
	}
	plan, err := current.PlanRebalance([]VShardNode{{ID: "node-a"}, {ID: "node-c"}}, generation+1, fencingToken+1)
	if err != nil || plan.MoveCount() == 0 {
		t.Fatalf("plan error = %v, moves = %d", err, plan.MoveCount())
	}
	return plan.Moves()[0]
}
