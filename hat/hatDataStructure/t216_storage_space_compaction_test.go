//go:build t216

package hatDataStructure

import (
	"context"
	"errors"
	"testing"
)

func TestT216StorageSpaceStatsExposeCompactionDebt(t *testing.T) {
	space, err := NewStorageSpace(StorageSpaceOptions{
		Name:             "events",
		Mode:             StorageSpaceOnDisk,
		Directory:        t.TempDir(),
		MemoryLimitBytes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer space.Close()
	if err := space.Set("key", []byte("first-value")); err != nil {
		t.Fatal(err)
	}
	if err := space.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := space.Set("key", []byte("second-value")); err != nil {
		t.Fatal(err)
	}
	if err := space.Flush(); err != nil {
		t.Fatal(err)
	}
	before := space.Stats()
	if before.DiskBytes <= 0 || before.LiveDiskBytes <= 0 || before.StaleDiskBytes <= 0 || before.CompactionDebtBytes != before.StaleDiskBytes {
		t.Fatalf("before compaction stats = %+v, want physical/live/stale accounting", before)
	}
	if err := space.Compact(); err != nil {
		t.Fatal(err)
	}
	after := space.Stats()
	if after.StaleDiskBytes != 0 || after.CompactionDebtBytes != 0 || after.DiskBytes >= before.DiskBytes {
		t.Fatalf("after compaction stats = %+v, before = %+v", after, before)
	}
	value, found, err := space.Get("key")
	if err != nil || !found || string(value) != "second-value" {
		t.Fatalf("Get after compaction = %q, %t, %v", value, found, err)
	}
}

func TestT216StorageSpaceCompactionSchedulerSelectsColdSpaces(t *testing.T) {
	cold, err := NewStorageSpace(StorageSpaceOptions{
		Name:             "cold",
		Mode:             StorageSpaceOnDisk,
		Directory:        t.TempDir(),
		MemoryLimitBytes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cold.Close()
	if err := cold.Set("key", []byte("before")); err != nil {
		t.Fatal(err)
	}
	if err := cold.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := cold.Set("key", []byte("after")); err != nil {
		t.Fatal(err)
	}
	if err := cold.Flush(); err != nil {
		t.Fatal(err)
	}
	hot, err := NewStorageSpace(StorageSpaceOptions{Name: "hot"})
	if err != nil {
		t.Fatal(err)
	}
	defer hot.Close()

	scheduler, err := NewStorageSpaceCompactionScheduler(StorageSpaceCompactionSchedulerOptions{
		MinStaleBytes:   1,
		MinStaleRatio:   0,
		MaxSpacesPerRun: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Register(cold); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Register(hot); err != nil {
		t.Fatal(err)
	}
	run, err := scheduler.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if run.Considered != 2 || run.Scheduled != 1 || run.Completed != 1 || run.Skipped != 1 || run.ReclaimedBytes <= 0 {
		t.Fatalf("compaction run = %+v, want one cold compaction and one skipped memtx space", run)
	}
	if got := cold.Stats().StaleDiskBytes; got != 0 {
		t.Fatalf("cold stale bytes after scheduler = %d, want 0", got)
	}
}

func TestT216StorageSpaceCompactionSchedulerValidatesAndHonorsContext(t *testing.T) {
	if _, err := NewStorageSpaceCompactionScheduler(StorageSpaceCompactionSchedulerOptions{MinStaleBytes: -1}); !errors.Is(err, ErrStorageSpaceCompactionOptionsInvalid) {
		t.Fatalf("negative stale bytes error = %v", err)
	}
	scheduler, err := NewStorageSpaceCompactionScheduler(StorageSpaceCompactionSchedulerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Register(nil); !errors.Is(err, ErrStorageSpaceCompactionSpaceInvalid) {
		t.Fatalf("nil Register error = %v", err)
	}
	if _, err := scheduler.Run(nil); !errors.Is(err, ErrStorageSpaceCompactionContextInvalid) {
		t.Fatalf("nil Run context error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := scheduler.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Run error = %v", err)
	}
}
