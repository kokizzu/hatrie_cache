package hatSchema

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestRollingSchemaCheckpointRoundTripRestoresProgress(t *testing.T) {
	plan, err := NewRollingSchemaPlan(rollingSchemaFixture(1, false), rollingSchemaFixture(2, true), []string{"node-b", "node-a"})
	if err != nil {
		t.Fatal(err)
	}
	deployment := plan.Begin()
	if err := deployment.Prepare("node-a"); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := plan.Checkpoint(deployment)
	if err != nil {
		t.Fatalf("Checkpoint() error = %v", err)
	}
	if checkpoint.PreviousFingerprint != plan.PreviousSchema().Fingerprint() || checkpoint.NextFingerprint != plan.NextSchema().Fingerprint() {
		t.Fatalf("checkpoint fingerprints = %#v, want plan fingerprints", checkpoint)
	}
	wire, err := checkpoint.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	decoded, err := DecodeRollingSchemaCheckpoint(wire)
	if err != nil {
		t.Fatalf("DecodeRollingSchemaCheckpoint() error = %v", err)
	}
	restored, err := plan.Restore(decoded)
	if err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	if got := restored.Snapshot(); len(got) != 2 || got[0].Phase != RollingSchemaPhasePrepared || got[1].Phase != RollingSchemaPhasePending {
		t.Fatalf("restored snapshot = %#v, want prepared/pending", got)
	}
	if err := restored.Activate("node-a"); err != nil {
		t.Fatal(err)
	}
	if got, _ := restored.Phase("node-a"); got != RollingSchemaPhaseActive {
		t.Fatalf("restored node-a phase = %v, want active", got)
	}
}

func TestRollingSchemaCheckpointRejectsTamperingAndPlanMismatch(t *testing.T) {
	plan, err := NewRollingSchemaPlan(rollingSchemaFixture(1, false), rollingSchemaFixture(2, true), []string{"node-a"})
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := plan.Checkpoint(plan.Begin())
	if err != nil {
		t.Fatal(err)
	}
	wire, err := checkpoint.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	tampered := append([]byte(nil), wire...)
	tampered[len(tampered)-1] ^= 1
	if _, err := DecodeRollingSchemaCheckpoint(tampered); !errors.Is(err, ErrRollingSchemaCheckpointChecksum) {
		t.Fatalf("tampered decode error = %v, want checksum error", err)
	}
	if _, err := DecodeRollingSchemaCheckpoint(wire[:len(wire)-1]); err == nil {
		t.Fatal("truncated checkpoint decoded without error")
	}
	otherPlan, err := NewRollingSchemaPlan(rollingSchemaFixture(1, false), rollingSchemaFixture(3, true), []string{"node-a"})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRollingSchemaCheckpoint(wire)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := otherPlan.Restore(decoded); !errors.Is(err, ErrRollingSchemaCheckpointInvalid) {
		t.Fatalf("mismatched plan restore error = %v, want checkpoint invalid", err)
	}
	if reencoded, err := decoded.MarshalBinary(); err != nil || !bytes.Equal(reencoded, wire) {
		t.Fatalf("checkpoint encoding is not deterministic: err=%v equal=%v", err, bytes.Equal(reencoded, wire))
	}
}

func TestRollingSchemaCheckpointPersistsLastStablePhaseDuringHook(t *testing.T) {
	plan, err := NewRollingSchemaPlan(rollingSchemaFixture(1, false), rollingSchemaFixture(2, true), []string{"node-a"})
	if err != nil {
		t.Fatal(err)
	}
	deployment := plan.Begin()
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- plan.Run(context.Background(), deployment,
			func(context.Context, string, Schema) error {
				close(started)
				<-release
				return nil
			},
			func(context.Context, string, Schema) error { return nil },
		)
	}()
	<-started
	checkpoint, err := plan.Checkpoint(deployment)
	if err != nil {
		t.Fatal(err)
	}
	if got := checkpoint.Nodes[0].Phase; got != RollingSchemaPhasePending {
		t.Fatalf("in-flight checkpoint phase = %v, want pending", got)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got, _ := deployment.Phase("node-a"); got != RollingSchemaPhaseActive {
		t.Fatalf("completed deployment phase = %v, want active", got)
	}
}

func TestRollingSchemaCheckpointRejectsNonCanonicalInput(t *testing.T) {
	plan, err := NewRollingSchemaPlan(rollingSchemaFixture(1, false), rollingSchemaFixture(2, true), []string{"node-a", "node-b"})
	if err != nil {
		t.Fatal(err)
	}
	valid, err := plan.Checkpoint(plan.Begin())
	if err != nil {
		t.Fatal(err)
	}
	valid.Nodes[0], valid.Nodes[1] = valid.Nodes[1], valid.Nodes[0]
	if _, err := valid.MarshalBinary(); !errors.Is(err, ErrRollingSchemaCheckpointInvalid) {
		t.Fatalf("unsorted checkpoint error = %v, want invalid", err)
	}
	valid, err = plan.Checkpoint(plan.Begin())
	if err != nil {
		t.Fatal(err)
	}
	valid.Nodes[0].Phase = RollingSchemaPhase(255)
	if _, err := valid.MarshalBinary(); !errors.Is(err, ErrRollingSchemaCheckpointInvalid) {
		t.Fatalf("invalid phase error = %v, want invalid", err)
	}
}
