package hatStorage

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestPersistentNodeEpochAdvancesAcrossRestarts(t *testing.T) {
	storagePath := filepath.Join(t.TempDir(), "cache")
	first, err := AcquirePersistentNodeEpoch(storagePath, "node-a")
	if err != nil {
		t.Fatal(err)
	}
	if first.Epoch() != 1 {
		t.Fatalf("first epoch = %d, want 1", first.Epoch())
	}
	if first.Owner() != "node-a" {
		t.Fatalf("first owner = %q, want node-a", first.Owner())
	}
	if err := first.Validate(); err != nil {
		t.Fatalf("first Validate() error = %v", err)
	}
	if err := first.Renew(); err != nil {
		t.Fatalf("first Renew() error = %v", err)
	}
	if _, err := AcquirePersistentNodeEpoch(storagePath, "node-b"); !errors.Is(err, ErrPersistentNodeEpochHeld) {
		t.Fatalf("second acquire error = %v, want ErrPersistentNodeEpochHeld", err)
	}

	firstEpoch := first.Epoch()
	if err := first.Release(); err != nil {
		t.Fatal(err)
	}
	if err := first.Release(); err != nil {
		t.Fatalf("second first Release() error = %v", err)
	}
	if err := first.Validate(); !errors.Is(err, ErrPersistentNodeEpochReleased) {
		t.Fatalf("released epoch validation error = %v, want ErrPersistentNodeEpochReleased", err)
	}
	if err := ValidatePersistentNodeEpoch(storagePath, firstEpoch); !errors.Is(err, ErrPersistentNodeEpochNotHeld) {
		t.Fatalf("released epoch token validation error = %v, want ErrPersistentNodeEpochNotHeld", err)
	}

	second, err := AcquirePersistentNodeEpoch(storagePath, "node-b")
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	if second.Epoch() <= firstEpoch {
		t.Fatalf("second epoch = %d, want greater than %d", second.Epoch(), firstEpoch)
	}
	if err := ValidatePersistentNodeEpoch(storagePath, firstEpoch); !errors.Is(err, ErrPersistentNodeEpochFenced) {
		t.Fatalf("stale epoch validation error = %v, want ErrPersistentNodeEpochFenced", err)
	}
	if err := ValidatePersistentNodeEpoch(storagePath, second.Epoch()); err != nil {
		t.Fatalf("current epoch validation error = %v", err)
	}

	info, err := InspectPersistentNodeEpoch(storagePath)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Active || info.Owner != "node-b" || info.Epoch != second.Epoch() {
		t.Fatalf("epoch info = %#v, want active node-b epoch %d", info, second.Epoch())
	}
}

func TestPersistentNodeEpochIsolatedFromShardLeases(t *testing.T) {
	storagePath := filepath.Join(t.TempDir(), "cache")
	epoch, err := AcquirePersistentNodeEpoch(storagePath, "node-a")
	if err != nil {
		t.Fatal(err)
	}
	defer epoch.Release()
	shard, err := AcquirePersistentShardLease(storagePath, "eu-west", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	defer shard.Release()
	if epoch.Epoch() != 1 || shard.Token() != 1 {
		t.Fatalf("epoch/token = %d/%d, want 1/1", epoch.Epoch(), shard.Token())
	}
}

func TestPersistentNodeEpochRejectsInvalidInput(t *testing.T) {
	if _, err := AcquirePersistentNodeEpoch("", "node-a"); !errors.Is(err, ErrPersistentNodeEpochInvalid) {
		t.Fatalf("missing storage path error = %v, want ErrPersistentNodeEpochInvalid", err)
	}
	if _, err := AcquirePersistentNodeEpoch(filepath.Join(t.TempDir(), "cache"), ""); !errors.Is(err, ErrPersistentNodeEpochInvalid) {
		t.Fatalf("missing owner error = %v, want ErrPersistentNodeEpochInvalid", err)
	}
	if err := ValidatePersistentNodeEpoch(filepath.Join(t.TempDir(), "cache"), 0); !errors.Is(err, ErrPersistentNodeEpochFenced) {
		t.Fatalf("zero epoch validation error = %v, want ErrPersistentNodeEpochFenced", err)
	}
}

func TestInspectPersistentNodeEpochDoesNotCreateMissingRecord(t *testing.T) {
	storagePath := filepath.Join(t.TempDir(), "cache")
	info, err := InspectPersistentNodeEpoch(storagePath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Active || info.Owner != "" || info.Epoch != 0 {
		t.Fatalf("missing epoch info = %#v, want inactive empty record", info)
	}
}

func BenchmarkPersistentNodeEpochAcquireRelease(b *testing.B) {
	storagePath := filepath.Join(b.TempDir(), "cache")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		epoch, err := AcquirePersistentNodeEpoch(storagePath, "node-a")
		if err != nil {
			b.Fatal(err)
		}
		if err := epoch.Release(); err != nil {
			b.Fatal(err)
		}
	}
}
