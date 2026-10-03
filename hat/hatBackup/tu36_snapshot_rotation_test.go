package hatBackup_test

import (
	"errors"
	"testing"
	"time"

	"hatrie_cache/hat/hatBackup"
)

func TestTU36PlanBackupRotationCombinesCountAndAge(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	base := testChainManifest("base", "", false, 10, 7, "a")
	base.CreatedAt = now.Add(-72 * time.Hour)
	previous := testChainManifest("previous", "base", true, 11, 7, "b")
	previous.CreatedAt = now.Add(-48 * time.Hour)
	recent := testChainManifest("recent", "previous", true, 12, 7, "c")
	recent.CreatedAt = now.Add(-2 * time.Hour)
	latest := testChainManifest("latest", "recent", true, 13, 7, "d")
	latest.CreatedAt = now.Add(-time.Hour)

	plan, err := hatBackup.PlanBackupRotation(
		[]hatBackup.BundleManifest{latest, base, recent, previous},
		"latest",
		hatBackup.BackupRotationPolicy{
			MaxBackups: 3,
			MaxAge:     24 * time.Hour,
			Now:        now,
		},
	)
	if err != nil {
		t.Fatalf("PlanBackupRotation() error = %v", err)
	}
	if got, want := plan.KeepBackupIDs, []string{"recent", "latest"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("kept backups = %#v, want %#v", got, want)
	}
	if len(plan.DeleteBackupIDs) != 2 || plan.DeleteBackupIDs[0] != "base" || plan.DeleteBackupIDs[1] != "previous" {
		t.Fatalf("deleted backups = %#v, want [base previous]", plan.DeleteBackupIDs)
	}
}

func TestTU36PlanBackupRotationRejectsInvalidPoliciesAndTooSmallBudget(t *testing.T) {
	base := testChainManifest("base", "", false, 10, 7, "a")
	latest := testChainManifest("latest", "base", true, 11, 7, "b")
	for _, policy := range []hatBackup.BackupRotationPolicy{
		{MaxBackups: -1},
		{MaxAge: -time.Second},
		{MaxObjectBytes: -1},
	} {
		if _, err := hatBackup.PlanBackupRotation([]hatBackup.BundleManifest{base, latest}, "latest", policy); !errors.Is(err, hatBackup.ErrBackupRotationInvalid) {
			t.Fatalf("invalid policy %#v error = %v, want ErrBackupRotationInvalid", policy, err)
		}
	}
	if _, err := hatBackup.PlanBackupRotation([]hatBackup.BundleManifest{base, latest}, "latest", hatBackup.BackupRotationPolicy{MaxObjectBytes: 1}); !errors.Is(err, hatBackup.ErrBackupRotationBudgetExceeded) {
		t.Fatalf("too-small budget error = %v, want ErrBackupRotationBudgetExceeded", err)
	}
}

func BenchmarkTU36PlanBackupRotation(b *testing.B) {
	manifests := benchmarkChainManifests(128)
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	for index := range manifests {
		manifests[index].CreatedAt = now.Add(-time.Duration(index) * time.Hour)
	}
	policy := hatBackup.BackupRotationPolicy{MaxBackups: 32, MaxAge: 48 * time.Hour, MaxObjectBytes: 64, Now: now}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := hatBackup.PlanBackupRotation(manifests, "backup-127", policy); err != nil {
			b.Fatal(err)
		}
	}
}

func TestTU36PlanBackupRotationHonorsUniqueObjectBudget(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	base := testChainManifest("base", "", false, 10, 7, "a")
	base.CreatedAt = now.Add(-2 * time.Hour)
	latest := testChainManifest("latest", "base", true, 11, 7, "b")
	latest.CreatedAt = now.Add(-time.Hour)

	plan, err := hatBackup.PlanBackupRotation(
		[]hatBackup.BundleManifest{base, latest},
		"latest",
		hatBackup.BackupRotationPolicy{
			MaxObjectBytes: 2,
			Now:            now,
		},
	)
	if err != nil {
		t.Fatalf("PlanBackupRotation() error = %v", err)
	}
	if len(plan.KeepBackupIDs) != 1 || plan.KeepBackupIDs[0] != "latest" {
		t.Fatalf("kept backups = %#v, want [latest]", plan.KeepBackupIDs)
	}
	if len(plan.KeepObjectHashes) != 1 || len(plan.DeleteObjectHashes) != 1 {
		t.Fatalf("object plan = %#v, want one kept and one deleted object", plan)
	}
}

func TestTU36PlanBackupRotationCountsSharedObjectsOnce(t *testing.T) {
	base := testChainManifest("base", "", false, 10, 7, "a")
	latest := testChainManifest("latest", "base", true, 11, 7, "b")
	latest.Files[0].SHA256 = base.Files[0].SHA256
	latest.Files[0].Size = base.Files[0].Size

	plan, err := hatBackup.PlanBackupRotation(
		[]hatBackup.BundleManifest{base, latest},
		"latest",
		hatBackup.BackupRotationPolicy{MaxBackups: 2, MaxObjectBytes: base.Files[0].Size},
	)
	if err != nil {
		t.Fatalf("PlanBackupRotation() error = %v", err)
	}
	if len(plan.KeepBackupIDs) != 2 || plan.KeepObjectBytes != base.Files[0].Size {
		t.Fatalf("shared-object plan = %#v, want two backups and %d bytes", plan, base.Files[0].Size)
	}
	if len(plan.KeepObjectKeys) != 1 || len(plan.DeleteObjectKeys) != 0 {
		t.Fatalf("shared-object keys = keep %#v delete %#v", plan.KeepObjectKeys, plan.DeleteObjectKeys)
	}
}
