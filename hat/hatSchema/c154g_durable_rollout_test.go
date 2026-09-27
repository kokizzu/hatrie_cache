package hatSchema

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestC154gRunWithCheckpointPersistsStablePhases(t *testing.T) {
	plan, err := NewRollingSchemaPlan(rollingSchemaFixture(1, false), rollingSchemaFixture(2, true), []string{"node-b", "node-a"})
	if err != nil {
		t.Fatal(err)
	}
	var checkpoints []RollingSchemaCheckpoint
	var events []string
	persist := RollingSchemaCheckpointPersistFunc(func(_ context.Context, checkpoint RollingSchemaCheckpoint) error {
		checkpoints = append(checkpoints, checkpoint)
		return nil
	})
	install := RollingSchemaInstallFunc(func(_ context.Context, node string, _ Schema) error {
		events = append(events, "install:"+node)
		return nil
	})
	activate := RollingSchemaActivateFunc(func(_ context.Context, node string, _ Schema) error {
		events = append(events, "activate:"+node)
		return nil
	})

	if err := plan.RunWithCheckpoint(context.Background(), plan.Begin(), persist, install, activate); err != nil {
		t.Fatal(err)
	}
	if want := []string{"install:node-a", "activate:node-a", "install:node-b", "activate:node-b"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	if len(checkpoints) != 4 {
		t.Fatalf("checkpoint count = %d, want 4", len(checkpoints))
	}
	if checkpoints[0].Nodes[0].Phase != RollingSchemaPhasePrepared || checkpoints[0].Nodes[1].Phase != RollingSchemaPhasePending {
		t.Fatalf("first checkpoint = %#v, want node-a prepared and node-b pending", checkpoints[0])
	}
	if checkpoints[1].Nodes[0].Phase != RollingSchemaPhaseActive || checkpoints[1].Nodes[1].Phase != RollingSchemaPhasePending {
		t.Fatalf("second checkpoint = %#v, want node-a active and node-b pending", checkpoints[1])
	}
	if checkpoints[3].Nodes[0].Phase != RollingSchemaPhaseActive || checkpoints[3].Nodes[1].Phase != RollingSchemaPhaseActive {
		t.Fatalf("final checkpoint = %#v, want all nodes active", checkpoints[3])
	}
}

func TestC154gRunWithCheckpointResumesFromLastDurableCheckpoint(t *testing.T) {
	plan, err := NewRollingSchemaPlan(rollingSchemaFixture(1, false), rollingSchemaFixture(2, true), []string{"node-a", "node-b"})
	if err != nil {
		t.Fatal(err)
	}
	var durable []RollingSchemaCheckpoint
	var persistCalls int
	persistFailure := errors.New("checkpoint store unavailable")
	installCalls := make(map[string]int)
	activateCalls := make(map[string]int)
	install := RollingSchemaInstallFunc(func(_ context.Context, node string, _ Schema) error {
		installCalls[node]++
		return nil
	})
	activate := RollingSchemaActivateFunc(func(_ context.Context, node string, _ Schema) error {
		activateCalls[node]++
		return nil
	})
	persist := RollingSchemaCheckpointPersistFunc(func(_ context.Context, checkpoint RollingSchemaCheckpoint) error {
		persistCalls++
		if persistCalls == 2 {
			return persistFailure
		}
		durable = append(durable, checkpoint)
		return nil
	})

	if err := plan.RunWithCheckpoint(context.Background(), plan.Begin(), persist, install, activate); !errors.Is(err, persistFailure) {
		t.Fatalf("first run error = %v, want checkpoint failure", err)
	}
	if len(durable) != 1 {
		t.Fatalf("durable checkpoint count = %d, want 1", len(durable))
	}
	recovered, err := plan.Restore(durable[0])
	if err != nil {
		t.Fatal(err)
	}
	persist = func(_ context.Context, checkpoint RollingSchemaCheckpoint) error {
		durable = append(durable, checkpoint)
		return nil
	}
	if err := plan.RunWithCheckpoint(context.Background(), recovered, persist, install, activate); err != nil {
		t.Fatal(err)
	}
	if !recovered.Complete() || installCalls["node-a"] != 1 || installCalls["node-b"] != 1 || activateCalls["node-a"] != 2 || activateCalls["node-b"] != 1 {
		t.Fatalf("recovered state = complete %v, install calls %#v, activate calls %#v", recovered.Complete(), installCalls, activateCalls)
	}
}

func TestC154gRunWithCheckpointRequiresCallback(t *testing.T) {
	plan, err := NewRollingSchemaPlan(rollingSchemaFixture(1, false), rollingSchemaFixture(2, true), []string{"node-a"})
	if err != nil {
		t.Fatal(err)
	}
	err = plan.RunWithCheckpoint(context.Background(), plan.Begin(), nil,
		func(context.Context, string, Schema) error { return nil },
		func(context.Context, string, Schema) error { return nil })
	if !errors.Is(err, ErrRollingSchemaCoordinatorInvalid) {
		t.Fatalf("nil callback error = %v, want ErrRollingSchemaCoordinatorInvalid", err)
	}
}
