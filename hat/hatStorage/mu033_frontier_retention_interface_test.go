package hatStorage_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"hatrie_cache/hat/hatPipeline"
	"hatrie_cache/hat/hatStorage"
)

var _ hatStorage.CompactionRetentionGate = (*hatPipeline.FrontierRetentionRegistry)(nil)

func TestMU033FrontierRetentionRegistryImplementsStorageGate(t *testing.T) {
	var gate hatStorage.CompactionRetentionGate = (*hatPipeline.FrontierRetentionRegistry)(nil)
	if gate == nil {
		t.Fatal("FrontierRetentionRegistry must satisfy CompactionRetentionGate")
	}
}

func TestMU033StorageCompactionWaitsForFrontierRetentionLease(t *testing.T) {
	frontiers, err := hatPipeline.NewFrontierRegistry(hatPipeline.FrontierRegistryOptions{MaxObjects: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := frontiers.Register("orders"); err != nil {
		t.Fatal(err)
	}
	if err := frontiers.Advance("orders", 10, 20); err != nil {
		t.Fatal(err)
	}
	retention, err := hatPipeline.NewFrontierRetentionRegistry(frontiers, hatPipeline.FrontierRetentionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := retention.Acquire("orders", 12)
	if err != nil {
		t.Fatal(err)
	}

	controller, err := hatStorage.NewCompactionController(hatStorage.CompactionControllerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var ran atomic.Bool
	job, queued, err := controller.Submit(hatStorage.CompactionRequest{
		Target:            "orders-part",
		RetentionGate:     retention,
		RetentionFrontier: "orders",
		RetentionBoundary: 15,
		Run: func(context.Context) error {
			ran.Store(true)
			return nil
		},
	})
	if err != nil || !queued {
		t.Fatalf("Submit() = %#v/%v/%v, want queued job", job, queued, err)
	}
	runDone := make(chan error, 1)
	go func() {
		_, runErr := controller.Run(context.Background())
		runDone <- runErr
	}()
	time.Sleep(20 * time.Millisecond)
	if ran.Load() {
		t.Fatal("compaction ran while an as-of lease blocked the boundary")
	}
	if err := retention.Release(lease); err != nil {
		t.Fatal(err)
	}
	if err := frontiers.Advance("orders", 15, 20); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not unblock after lease release and frontier progress")
	}
	if !ran.Load() {
		t.Fatal("compaction callback did not run after retention became safe")
	}
}
