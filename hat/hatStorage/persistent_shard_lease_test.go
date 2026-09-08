package hatStorage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPersistentShardLeaseAcquireRenewAndFence(t *testing.T) {
	storagePath := filepath.Join(t.TempDir(), "cache")
	first, err := AcquirePersistentShardLease(storagePath, "eu-west", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	if first.Token() != 1 {
		t.Fatalf("first token = %d, want 1", first.Token())
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
	if _, err := AcquirePersistentShardLease(storagePath, "eu-west", "node-b"); !errors.Is(err, ErrPersistentShardLeaseHeld) {
		t.Fatalf("second acquire error = %v, want ErrPersistentShardLeaseHeld", err)
	}

	if err := first.Release(); err != nil {
		t.Fatalf("first Release() error = %v", err)
	}
	if err := first.Release(); err != nil {
		t.Fatalf("second first Release() error = %v, want idempotent release", err)
	}

	second, err := AcquirePersistentShardLease(storagePath, "eu-west", "node-b")
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	if second.Token() <= first.Token() {
		t.Fatalf("second token = %d, want greater than %d", second.Token(), first.Token())
	}
	if err := second.Validate(); err != nil {
		t.Fatalf("second Validate() error = %v", err)
	}
	if err := ValidatePersistentShardLeaseToken(storagePath, "eu-west", first.Token()); !errors.Is(err, ErrPersistentShardLeaseFenced) {
		t.Fatalf("stale token validation error = %v, want ErrPersistentShardLeaseFenced", err)
	}
	if err := ValidatePersistentShardLeaseToken(storagePath, "eu-west", second.Token()); err != nil {
		t.Fatalf("current token validation error = %v", err)
	}

	info, err := InspectPersistentShardLease(storagePath, "eu-west")
	if err != nil {
		t.Fatal(err)
	}
	if !info.Active || info.Owner != "node-b" || info.Token != second.Token() {
		t.Fatalf("lease info = %#v, want active node-b token %d", info, second.Token())
	}
}

func TestPersistentShardLeaseRejectsInvalidInput(t *testing.T) {
	for name, values := range map[string][3]string{
		"missing storage path": {"", "eu-west", "node-a"},
		"missing shard":        {"/tmp/cache", "", "node-a"},
		"missing owner":        {"/tmp/cache", "eu-west", ""},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := AcquirePersistentShardLease(values[0], values[1], values[2]); !errors.Is(err, ErrPersistentShardLeaseInvalid) {
				t.Fatalf("AcquirePersistentShardLease() error = %v, want ErrPersistentShardLeaseInvalid", err)
			}
		})
	}
}

func TestPersistentShardLeaseSeparatesShardsAndRejectsCorruptState(t *testing.T) {
	storagePath := filepath.Join(t.TempDir(), "cache")
	first, err := AcquirePersistentShardLease(storagePath, "eu-west", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	other, err := AcquirePersistentShardLease(storagePath, "us-east", "node-b")
	if err != nil {
		t.Fatal(err)
	}
	if first.Token() != 1 || other.Token() != 1 {
		t.Fatalf("different shard tokens = %d and %d, want both 1", first.Token(), other.Token())
	}
	if err := first.Release(); err != nil {
		t.Fatal(err)
	}
	info, err := InspectPersistentShardLease(storagePath, "eu-west")
	if err != nil {
		t.Fatal(err)
	}
	if info.Active || info.Owner != "node-a" || info.Token != 1 {
		t.Fatalf("released lease info = %#v, want inactive node-a token 1", info)
	}
	if err := ValidatePersistentShardLeaseToken(storagePath, "eu-west", 1); !errors.Is(err, ErrPersistentShardLeaseNotHeld) {
		t.Fatalf("released token validation error = %v, want ErrPersistentShardLeaseNotHeld", err)
	}
	if err := other.Release(); err != nil {
		t.Fatal(err)
	}

	_, statePath := persistentShardLeasePaths(storagePath, "eu-west")
	if err := os.WriteFile(statePath, []byte(`{"version":1,"shard_id":"eu-west","token":0}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := AcquirePersistentShardLease(storagePath, "eu-west", "node-c"); !errors.Is(err, ErrPersistentShardLeaseInvalid) {
		t.Fatalf("corrupt state acquire error = %v, want ErrPersistentShardLeaseInvalid", err)
	}
}

func TestInspectPersistentShardLeaseDoesNotCreateMissingRecord(t *testing.T) {
	storagePath := filepath.Join(t.TempDir(), "cache")
	lease, err := AcquirePersistentShardLease(storagePath, "eu-west", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}

	lockPath, statePath := persistentShardLeasePaths(storagePath, "us-east")
	info, err := InspectPersistentShardLease(storagePath, "us-east")
	if err != nil {
		t.Fatal(err)
	}
	if info.ShardID != "us-east" || info.Active || info.Owner != "" || info.Token != 0 {
		t.Fatalf("missing lease info = %#v, want inactive us-east with no owner/token", info)
	}
	if _, err := os.Stat(lockPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing lease lock stat error = %v, want os.ErrNotExist", err)
	}
	if _, err := os.Stat(statePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing lease state stat error = %v, want os.ErrNotExist", err)
	}
}

func BenchmarkPersistentShardLeaseAcquireRelease(b *testing.B) {
	storagePath := filepath.Join(b.TempDir(), "cache")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		lease, err := AcquirePersistentShardLease(storagePath, "eu-west", "node-a")
		if err != nil {
			b.Fatal(err)
		}
		if err := lease.Release(); err != nil {
			b.Fatal(err)
		}
	}
}
