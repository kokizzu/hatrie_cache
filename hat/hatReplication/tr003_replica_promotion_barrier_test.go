package hatReplication

import (
	"errors"
	"sync"
	"testing"
)

func TestTR003ReplicaPromotionBarrierRequiresCatchupFence(t *testing.T) {
	barrier, err := NewReplicaPromotionBarrier(ReplicaPromotionBarrierOptions{MaxReplicas: 4})
	if err != nil {
		t.Fatalf("NewReplicaPromotionBarrier() error = %v", err)
	}
	if err := barrier.ObserveSource(100); err != nil {
		t.Fatalf("ObserveSource() error = %v", err)
	}
	if err := barrier.ObserveReplica("standby-a", 99); err != nil {
		t.Fatalf("ObserveReplica() error = %v", err)
	}
	if _, err := barrier.Capture("standby-a"); !errors.Is(err, ErrReplicaPromotionBarrierNotCaughtUp) {
		t.Fatalf("Capture(lagging) error = %v, want catch-up error", err)
	}

	if err := barrier.ObserveReplica("standby-a", 100); err != nil {
		t.Fatalf("ObserveReplica(caught up) error = %v", err)
	}
	token, err := barrier.Capture("standby-a")
	if err != nil {
		t.Fatalf("Capture(caught up) error = %v", err)
	}
	if token.RequiredSequence != 100 || token.AppliedSequence != 100 {
		t.Fatalf("promotion token = %#v, want sequence 100", token)
	}

	if err := barrier.ObserveSource(101); err != nil {
		t.Fatalf("ObserveSource(advance) error = %v", err)
	}
	if _, err := barrier.Promote(token); !errors.Is(err, ErrReplicaPromotionBarrierSourceAdvanced) {
		t.Fatalf("Promote(stale source fence) error = %v, want source-advanced error", err)
	}
	if err := barrier.ObserveReplica("standby-a", 101); err != nil {
		t.Fatalf("ObserveReplica(second catch-up) error = %v", err)
	}

	token, err = barrier.Capture("standby-a")
	if err != nil {
		t.Fatalf("Capture(second fence) error = %v", err)
	}
	result, err := barrier.Promote(token)
	if err != nil {
		t.Fatalf("Promote(caught up) error = %v", err)
	}
	if result.Node != "standby-a" || result.SourceSequence != 101 || result.AppliedSequence != 101 || result.Generation != 1 {
		t.Fatalf("promotion result = %#v, want standby-a at generation 1", result)
	}
	if _, err := barrier.Promote(token); !errors.Is(err, ErrReplicaPromotionBarrierStaleToken) {
		t.Fatalf("Promote(reused token) error = %v, want stale-token error", err)
	}
}

func TestTR003ReplicaPromotionBarrierRejectsRegressionsAndCopiesSnapshot(t *testing.T) {
	barrier, err := NewReplicaPromotionBarrier(ReplicaPromotionBarrierOptions{MaxReplicas: 1})
	if err != nil {
		t.Fatalf("NewReplicaPromotionBarrier() error = %v", err)
	}
	if err := barrier.ObserveSource(10); err != nil {
		t.Fatalf("ObserveSource() error = %v", err)
	}
	if err := barrier.ObserveReplica("standby-a", 10); err != nil {
		t.Fatalf("ObserveReplica() error = %v", err)
	}
	if err := barrier.ObserveSource(9); !errors.Is(err, ErrReplicaPromotionBarrierSequenceRegression) {
		t.Fatalf("source regression error = %v, want regression error", err)
	}
	if err := barrier.ObserveReplica("standby-a", 9); !errors.Is(err, ErrReplicaPromotionBarrierSequenceRegression) {
		t.Fatalf("replica regression error = %v, want regression error", err)
	}
	if err := barrier.ObserveReplica("standby-b", 10); !errors.Is(err, ErrReplicaPromotionBarrierOptionsInvalid) {
		t.Fatalf("replica limit error = %v, want options error", err)
	}

	snapshot := barrier.Snapshot()
	if snapshot.Generation != 0 || snapshot.SourceSequence != 10 || len(snapshot.Replicas) != 1 {
		t.Fatalf("snapshot = %#v, want generation 0/source 10/one replica", snapshot)
	}
	snapshot.Replicas[0].Node = "tampered"
	if got := barrier.Snapshot().Replicas[0].Node; got == "tampered" {
		t.Fatal("Snapshot() exposed mutable replica state")
	}
}

func TestTR003ReplicaPromotionBarrierValidatesConfigurationAndNilReceiver(t *testing.T) {
	if _, err := NewReplicaPromotionBarrier(ReplicaPromotionBarrierOptions{MaxReplicas: -1}); !errors.Is(err, ErrReplicaPromotionBarrierOptionsInvalid) {
		t.Fatalf("negative maximum error = %v, want options error", err)
	}
	var barrier *ReplicaPromotionBarrier
	if err := barrier.ObserveSource(1); !errors.Is(err, ErrReplicaPromotionBarrierNil) {
		t.Fatalf("nil ObserveSource() error = %v, want nil error", err)
	}
	if err := barrier.ObserveReplica("standby-a", 1); !errors.Is(err, ErrReplicaPromotionBarrierNil) {
		t.Fatalf("nil ObserveReplica() error = %v, want nil error", err)
	}
	if _, err := barrier.Capture("standby-a"); !errors.Is(err, ErrReplicaPromotionBarrierNil) {
		t.Fatalf("nil Capture() error = %v, want nil error", err)
	}
	if _, err := barrier.Promote(ReplicaPromotionToken{}); !errors.Is(err, ErrReplicaPromotionBarrierNil) {
		t.Fatalf("nil Promote() error = %v, want nil error", err)
	}
}

func TestTR003ReplicaPromotionBarrierConcurrentSnapshots(t *testing.T) {
	barrier, err := NewReplicaPromotionBarrier(ReplicaPromotionBarrierOptions{})
	if err != nil {
		t.Fatalf("NewReplicaPromotionBarrier() error = %v", err)
	}
	var waitGroup sync.WaitGroup
	waitGroup.Add(2)
	go func() {
		defer waitGroup.Done()
		for sequence := uint64(1); sequence <= 128; sequence++ {
			if err := barrier.ObserveSource(sequence); err != nil {
				t.Errorf("ObserveSource() error = %v", err)
			}
			if err := barrier.ObserveReplica("standby-a", sequence); err != nil {
				t.Errorf("ObserveReplica() error = %v", err)
			}
		}
	}()
	go func() {
		defer waitGroup.Done()
		for index := 0; index < 256; index++ {
			if snapshot := barrier.Snapshot(); len(snapshot.Replicas) > 1 {
				t.Errorf("snapshot replica count = %d, want at most one", len(snapshot.Replicas))
			}
		}
	}()
	waitGroup.Wait()
	if snapshot := barrier.Snapshot(); snapshot.SourceSequence != 128 || snapshot.Replicas[0].AppliedSequence != 128 {
		t.Fatalf("final snapshot = %#v, want sequence 128", snapshot)
	}
}
