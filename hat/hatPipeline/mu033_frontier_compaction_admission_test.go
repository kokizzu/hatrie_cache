package hatPipeline

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMU33FrontierRetentionCompactionPermitBlocksOlderLeases(t *testing.T) {
	frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := frontiers.Register("events"); err != nil {
		t.Fatal(err)
	}
	if err := frontiers.Advance("events", 5, 10); err != nil {
		t.Fatal(err)
	}
	retention, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := retention.Acquire("events", 6)
	if err != nil {
		t.Fatal(err)
	}
	permitResult := make(chan *FrontierCompactionPermit, 1)
	errResult := make(chan error, 1)
	go func() {
		permit, err := retention.BeginCompaction(context.Background(), "events", 8)
		permitResult <- permit
		errResult <- err
	}()
	select {
	case err := <-errResult:
		t.Fatalf("BeginCompaction returned while lease was active: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	if err := frontiers.Advance("events", 8, 10); err != nil {
		t.Fatal(err)
	}
	if err := retention.Release(lease); err != nil {
		t.Fatal(err)
	}
	var permit *FrontierCompactionPermit
	select {
	case permit = <-permitResult:
	case <-time.After(time.Second):
		t.Fatal("BeginCompaction did not acquire after lease release")
	}
	if err := <-errResult; err != nil {
		t.Fatal(err)
	}
	snapshot, err := retention.Snapshot("events")
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.CompactionActive || snapshot.CompactionBoundary != 8 || snapshot.SafeCompactionBefore != 0 {
		t.Fatalf("Snapshot(during compaction) = %+v", snapshot)
	}
	if err := permit.Release(); err != nil {
		t.Fatal(err)
	}
	if err := permit.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := retention.Acquire("events", 8); err != nil {
		t.Fatalf("Acquire(after compaction) error = %v", err)
	}
}

func TestMU33FrontierRetentionCompactionPermitHonorsCancellation(t *testing.T) {
	frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := frontiers.Register("events"); err != nil {
		t.Fatal(err)
	}
	if err := frontiers.Advance("events", 5, 10); err != nil {
		t.Fatal(err)
	}
	retention, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := retention.Acquire("events", 6)
	if err != nil {
		t.Fatal(err)
	}
	if err := frontiers.Advance("events", 8, 10); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errResult := make(chan error, 1)
	go func() {
		_, err := retention.BeginCompaction(ctx, "events", 8)
		errResult <- err
	}()
	select {
	case err := <-errResult:
		t.Fatalf("BeginCompaction returned before cancellation: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-errResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("BeginCompaction(canceled) error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("BeginCompaction did not honor cancellation")
	}
	if err := retention.Release(lease); err != nil {
		t.Fatal(err)
	}
}
