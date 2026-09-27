package hatCache

import (
	"context"
	"errors"
	"testing"

	"hatrie_cache/hat/hatSchema"
)

func TestC154ReplicationSchemaRolloutSwitchesContractsAtCompletion(t *testing.T) {
	previous := replicationSchemaFixture()
	next := previous.Clone()
	next.Version++
	rollout, err := NewReplicationSchemaRollout(previous, next, []string{"node-a", "node-b"})
	if err != nil {
		t.Fatalf("NewReplicationSchemaRollout() error = %v", err)
	}
	previousContract := NewReplicationSchemaContract(previous)
	nextContract := NewReplicationSchemaContract(next)
	if !rollout.Accepts(previousContract) || !rollout.Accepts(nextContract) {
		t.Fatalf("rollout should accept both contracts during transition")
	}
	if got, ok := rollout.Contract("node-a"); !ok || got != previousContract {
		t.Fatalf("pending node contract = %#v/%v, want previous %#v/true", got, ok, previousContract)
	}
	if err := rollout.Activate("node-a"); err == nil {
		t.Fatal("Activate() error = nil, want prepare-before-activate rejection")
	}
	if err := rollout.Prepare("node-a"); err != nil {
		t.Fatalf("Prepare(node-a) error = %v", err)
	}
	if err := rollout.Activate("node-a"); err != nil {
		t.Fatalf("Activate(node-a) error = %v", err)
	}
	if got, ok := rollout.Contract("node-a"); !ok || got != nextContract {
		t.Fatalf("active node contract = %#v/%v, want next %#v/true", got, ok, nextContract)
	}
	if rollout.Complete() {
		t.Fatal("rollout complete after only one node")
	}
	if !rollout.Accepts(previousContract) {
		t.Fatal("rollout rejected previous contract before all nodes activated")
	}
	if err := rollout.Run(t.Context(), func(_ context.Context, _ string, _ hatSchema.Schema) error { return nil }, func(_ context.Context, _ string, _ hatSchema.Schema) error { return nil }); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !rollout.Complete() {
		t.Fatal("rollout not complete after Run()")
	}
	if rollout.Accepts(previousContract) || !rollout.Accepts(nextContract) {
		t.Fatal("rollout contract acceptance did not retire previous contract")
	}
	policy, err := rollout.CompatibilityPolicy()
	if err != nil {
		t.Fatalf("CompatibilityPolicy() error = %v", err)
	}
	if policy.Accepts(previousContract) || !policy.Accepts(nextContract) {
		t.Fatalf("completed policy acceptance = previous %v next %v, want false/true", policy.Accepts(previousContract), policy.Accepts(nextContract))
	}
}

func TestC154ReplicationSchemaRolloutRejectsInvalidAndUnknownOperations(t *testing.T) {
	previous := replicationSchemaFixture()
	next := previous.Clone()
	next.Version++
	if _, err := NewReplicationSchemaRollout(previous, next, nil); !errors.Is(err, ErrReplicationSchemaRolloutInvalid) {
		t.Fatalf("nil nodes error = %v, want %v", err, ErrReplicationSchemaRolloutInvalid)
	}
	var nilRollout *ReplicationSchemaRollout
	if err := nilRollout.Prepare("node-a"); !errors.Is(err, ErrReplicationSchemaRolloutNil) {
		t.Fatalf("nil Prepare() error = %v, want %v", err, ErrReplicationSchemaRolloutNil)
	}
	if _, ok := nilRollout.Contract("node-a"); ok || nilRollout.Complete() || nilRollout.Accepts(ReplicationSchemaContract{}) {
		t.Fatal("nil rollout returned a usable state")
	}
	rollout, err := NewReplicationSchemaRollout(previous, next, []string{"node-a"})
	if err != nil {
		t.Fatalf("NewReplicationSchemaRollout() error = %v", err)
	}
	if _, ok := rollout.Contract("unknown"); ok {
		t.Fatal("unknown node returned a contract")
	}
	if _, ok := rollout.Phase("unknown"); ok {
		t.Fatal("unknown node returned a phase")
	}
	nodes := rollout.Nodes()
	nodes[0] = "mutated"
	if rollout.Nodes()[0] != "node-a" {
		t.Fatal("Nodes() exposed internal storage")
	}
}

func BenchmarkC154ReplicationSchemaRollout(b *testing.B) {
	previous := replicationSchemaFixture()
	next := previous.Clone()
	next.Version++
	base, err := NewReplicationSchemaRollout(previous, next, []string{"node-a", "node-b", "node-c", "node-d"})
	if err != nil {
		b.Fatal(err)
	}
	plan := base.plan
	install := func(context.Context, string, hatSchema.Schema) error { return nil }
	activate := func(context.Context, string, hatSchema.Schema) error { return nil }
	b.ReportAllocs()
	b.Run("manual-plan", func(b *testing.B) {
		for range b.N {
			deployment := plan.Begin()
			if err := plan.Run(context.Background(), deployment, install, activate); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("replication-rollout", func(b *testing.B) {
		for range b.N {
			rollout := base.Begin()
			if err := rollout.Run(context.Background(), install, activate); err != nil {
				b.Fatal(err)
			}
		}
	})
}
