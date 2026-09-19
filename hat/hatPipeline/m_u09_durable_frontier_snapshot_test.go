package hatPipeline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestDurableFrontierSnapshotRoundTripsAndContinuesMonotoneAdvance(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontiers.bin")
	store, err := NewFrontierSnapshotFileStore(FrontierSnapshotFileStoreOptions{Path: path})
	if err != nil {
		t.Fatalf("NewFrontierSnapshotFileStore() error = %v", err)
	}
	registry, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("orders"); err != nil {
		t.Fatal(err)
	}
	if err := registry.Advance("orders", 10, 20); err != nil {
		t.Fatal(err)
	}
	if err := registry.SaveDurableSnapshot(context.Background(), store); err != nil {
		t.Fatalf("SaveDurableSnapshot() error = %v", err)
	}

	restored, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	found, err := restored.RestoreDurableSnapshot(context.Background(), store)
	if err != nil || !found {
		t.Fatalf("RestoreDurableSnapshot() = found %v, error %v; want true, nil", found, err)
	}
	if got, want := restored.SnapshotAll(), registry.SnapshotAll(); !reflect.DeepEqual(got, want) {
		t.Fatalf("restored snapshots = %#v, want %#v", got, want)
	}
	if err := restored.Advance("orders", 11, 21); err != nil {
		t.Fatalf("restored Advance() error = %v", err)
	}
}

func TestDurableFrontierSnapshotDistinguishesMissingAndCorruptFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontiers.bin")
	store, err := NewFrontierSnapshotFileStore(FrontierSnapshotFileStoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	found, err := registry.RestoreDurableSnapshot(context.Background(), store)
	if err != nil || found {
		t.Fatalf("missing RestoreDurableSnapshot() = found %v, error %v; want false, nil", found, err)
	}
	if err := os.WriteFile(path, []byte("not-a-frontier-snapshot"), 0600); err != nil {
		t.Fatal(err)
	}
	found, err = registry.RestoreDurableSnapshot(context.Background(), store)
	if !errors.Is(err, ErrFrontierSnapshotInvalid) || found {
		t.Fatalf("corrupt RestoreDurableSnapshot() = found %v, error %v; want false and invalid error", found, err)
	}
	if got := registry.SnapshotAll(); len(got) != 0 {
		t.Fatalf("corrupt restore mutated registry: %#v", got)
	}
}

func TestDurableFrontierSnapshotIsAtomicAndUsesPrivateFileMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontiers.bin")
	store, err := NewFrontierSnapshotFileStore(FrontierSnapshotFileStoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("a"); err != nil {
		t.Fatal(err)
	}
	if err := registry.Advance("a", 1, 2); err != nil {
		t.Fatal(err)
	}
	if err := registry.SaveDurableSnapshot(context.Background(), store); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0600 {
		t.Fatalf("snapshot mode = %o, want 600", got)
	}
	other, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := other.Register("existing"); err != nil {
		t.Fatal(err)
	}
	if found, err := other.RestoreDurableSnapshot(context.Background(), store); !errors.Is(err, ErrFrontierSnapshotNotEmpty) || found {
		t.Fatalf("non-empty restore = found %v, error %v; want false and not-empty error", found, err)
	}
	if got, ok := other.Snapshot("existing"); !ok || got.ID != "existing" {
		t.Fatalf("non-empty restore changed existing registry: %#v/%v", got, ok)
	}
}

func TestDurableFrontierSnapshotValidatesOptionsAndCancellation(t *testing.T) {
	if _, err := NewFrontierSnapshotFileStore(FrontierSnapshotFileStoreOptions{}); !errors.Is(err, ErrFrontierSnapshotStorePathEmpty) {
		t.Fatalf("empty path error = %v, want ErrFrontierSnapshotStorePathEmpty", err)
	}
	if _, err := NewFrontierSnapshotFileStore(FrontierSnapshotFileStoreOptions{Path: "x", MaxBytes: 0}); err != nil {
		t.Fatalf("zero MaxBytes error = %v", err)
	}
	if _, err := NewFrontierSnapshotFileStore(FrontierSnapshotFileStoreOptions{Path: "x", MaxBytes: 1}); err != nil {
		t.Fatalf("small MaxBytes error = %v", err)
	}
	if _, err := NewFrontierSnapshotFileStore(FrontierSnapshotFileStoreOptions{Path: "x", MaxBytes: maxFrontierSnapshotBytes + 1}); !errors.Is(err, ErrFrontierSnapshotStoreOptionsInvalid) {
		t.Fatalf("large MaxBytes error = %v, want ErrFrontierSnapshotStoreOptionsInvalid", err)
	}
	path := filepath.Join(t.TempDir(), "frontiers.bin")
	store, err := NewFrontierSnapshotFileStore(FrontierSnapshotFileStoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := registry.SaveDurableSnapshot(ctx, store); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled SaveDurableSnapshot() error = %v, want context.Canceled", err)
	}
	if _, err := store.Load(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Load() error = %v, want context.Canceled", err)
	}
	if _, err := registry.RestoreDurableSnapshot(ctx, store); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled RestoreDurableSnapshot() error = %v, want context.Canceled", err)
	}
}

func TestDurableFrontierSnapshotBoundsPayloadBeforeAllocation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontiers.bin")
	store, err := NewFrontierSnapshotFileStore(FrontierSnapshotFileStoreOptions{Path: path, MaxBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(context.Background(), []byte{1, 2}); !errors.Is(err, ErrFrontierSnapshotStorePayloadTooLarge) {
		t.Fatalf("oversized Save() error = %v, want ErrFrontierSnapshotStorePayloadTooLarge", err)
	}
	if err := os.WriteFile(path, []byte{1, 2}, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(context.Background()); !errors.Is(err, ErrFrontierSnapshotStorePayloadTooLarge) {
		t.Fatalf("oversized Load() error = %v, want ErrFrontierSnapshotStorePayloadTooLarge", err)
	}
}
