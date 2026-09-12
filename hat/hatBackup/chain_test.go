package hatBackup_test

import (
	"fmt"
	"strings"
	"testing"

	"hatrie_cache/hat/hatBackup"
)

func TestPlanBackupChainBuildsBaseToLatestOrder(t *testing.T) {
	base := testChainManifest("base", "", false, 10, 7, "a")
	latest := testChainManifest("latest", "base", true, 12, 7, "b")

	plan, err := hatBackup.PlanBackupChain([]hatBackup.BundleManifest{latest, base}, "latest")
	if err != nil {
		t.Fatalf("PlanBackupChain() error = %v", err)
	}
	if plan.BaseBackupID != "base" || plan.LatestBackupID != "latest" {
		t.Fatalf("plan endpoints = %#v, want base/latest", plan)
	}
	if got := len(plan.Manifests); got != 2 || plan.Manifests[0].BackupID != "base" || plan.Manifests[1].BackupID != "latest" {
		t.Fatalf("plan manifests = %#v, want base then latest", plan.Manifests)
	}
	if plan.JournalSequenceStart != 10 || plan.JournalSequenceEnd != 12 || plan.ObjectCount != 2 || plan.ObjectBytes != 2 {
		t.Fatalf("plan summary = %#v", plan)
	}
}

func TestPlanBackupRetentionProtectsKeptObjects(t *testing.T) {
	base := testChainManifest("base", "", false, 10, 7, "a")
	latest := testChainManifest("latest", "base", true, 12, 7, "b")

	plan, err := hatBackup.PlanBackupRetention([]hatBackup.BundleManifest{latest, base}, "latest", 1)
	if err != nil {
		t.Fatalf("PlanBackupRetention() error = %v", err)
	}
	if len(plan.KeepBackupIDs) != 1 || plan.KeepBackupIDs[0] != "latest" {
		t.Fatalf("kept backups = %#v, want latest", plan.KeepBackupIDs)
	}
	if len(plan.DeleteBackupIDs) != 1 || plan.DeleteBackupIDs[0] != "base" {
		t.Fatalf("deleted backups = %#v, want base", plan.DeleteBackupIDs)
	}
	if len(plan.KeepObjectHashes) != 1 || len(plan.DeleteObjectHashes) != 1 {
		t.Fatalf("object plan = %#v", plan)
	}
}

func TestPlanBackupChainRejectsUnsafeRelationships(t *testing.T) {
	tests := []struct {
		name      string
		manifests []hatBackup.BundleManifest
		want      string
	}{
		{
			name:      "missing parent",
			manifests: []hatBackup.BundleManifest{testChainManifest("latest", "missing", true, 12, 7, "a")},
			want:      "parent",
		},
		{
			name: "generation mismatch",
			manifests: []hatBackup.BundleManifest{
				testChainManifest("base", "", false, 10, 7, "a"),
				testChainManifest("latest", "base", true, 12, 8, "b"),
			},
			want: "generation",
		},
		{
			name: "sequence regression",
			manifests: []hatBackup.BundleManifest{
				testChainManifest("base", "", false, 12, 7, "a"),
				testChainManifest("latest", "base", true, 11, 7, "b"),
			},
			want: "sequence",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := hatBackup.PlanBackupChain(test.manifests, "latest"); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("PlanBackupChain() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestPlanBackupChainRejectsMalformedObjectsAndCycles(t *testing.T) {
	tests := []struct {
		name  string
		build func() []hatBackup.BundleManifest
		want  string
	}{
		{
			name: "cycle",
			build: func() []hatBackup.BundleManifest {
				base := testChainManifest("base", "latest", true, 10, 7, "a")
				latest := testChainManifest("latest", "base", true, 12, 7, "b")
				return []hatBackup.BundleManifest{base, latest}
			},
			want: "cycle",
		},
		{
			name: "duplicate file",
			build: func() []hatBackup.BundleManifest {
				base := testChainManifest("base", "", false, 10, 7, "a")
				base.Files = append(base.Files, base.Files[0])
				return []hatBackup.BundleManifest{base}
			},
			want: "duplicate file",
		},
		{
			name: "unsafe path",
			build: func() []hatBackup.BundleManifest {
				base := testChainManifest("base", "", false, 10, 7, "a")
				base.Files[0].Path = "../outside"
				return []hatBackup.BundleManifest{base}
			},
			want: "unsafe",
		},
		{
			name: "invalid hash",
			build: func() []hatBackup.BundleManifest {
				base := testChainManifest("base", "", false, 10, 7, "a")
				base.Files[0].SHA256 = "not-a-sha256"
				return []hatBackup.BundleManifest{base}
			},
			want: "SHA-256",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := hatBackup.PlanBackupChain(test.build(), "latest"); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("PlanBackupChain() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestPlanBackupChainCopiesManifestData(t *testing.T) {
	input := []hatBackup.BundleManifest{testChainManifest("base", "", false, 10, 7, "a")}
	plan, err := hatBackup.PlanBackupChain(input, "base")
	if err != nil {
		t.Fatalf("PlanBackupChain() error = %v", err)
	}
	plan.Manifests[0].Files[0].Path = "changed"
	if input[0].Files[0].Path == "changed" {
		t.Fatal("PlanBackupChain() retained input file backing")
	}
}

func TestPlanBackupRetentionRejectsMixedStorage(t *testing.T) {
	base := testChainManifest("base", "", false, 10, 7, "a")
	latest := testChainManifest("latest", "base", true, 12, 7, "b")
	other := testChainManifest("other", "", false, 14, 8, "c")
	if _, err := hatBackup.PlanBackupRetention([]hatBackup.BundleManifest{base, latest, other}, "latest", 1); err == nil || !strings.Contains(err.Error(), "mixed storage") {
		t.Fatalf("PlanBackupRetention() error = %v, want mixed storage rejection", err)
	}
}

func BenchmarkPlanBackupChain(b *testing.B) {
	manifests := benchmarkChainManifests(128)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := hatBackup.PlanBackupChain(manifests, "backup-127"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPlanBackupRetention(b *testing.B) {
	manifests := benchmarkChainManifests(128)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := hatBackup.PlanBackupRetention(manifests, "backup-127", 32); err != nil {
			b.Fatal(err)
		}
	}
}

func testChainManifest(id, parent string, incremental bool, sequence, generation uint64, objectSuffix string) hatBackup.BundleManifest {
	return hatBackup.BundleManifest{
		Version:           hatBackup.BundleVersion,
		Mode:              hatBackup.ModePebbleIncremental,
		BackupID:          id,
		ParentBackupID:    parent,
		Incremental:       incremental,
		Store:             "cache.leveldb",
		StorageBackend:    "pebble",
		StorageFormat:     "pebble-v1",
		StorageGeneration: generation,
		StorageIdentity:   "store-a",
		JournalSequence:   sequence,
		Files: []hatBackup.BundleFile{{
			Path:   "cache.leveldb/" + objectSuffix,
			Size:   1 + int64(sequence%2),
			SHA256: strings.Repeat(objectSuffix, 64),
		}},
	}
}

func benchmarkChainManifests(count int) []hatBackup.BundleManifest {
	manifests := make([]hatBackup.BundleManifest, count)
	for index := range manifests {
		parent := ""
		incremental := false
		if index > 0 {
			parent = fmt.Sprintf("backup-%d", index-1)
			incremental = true
		}
		digit := string("0123456789abcdef"[index%16])
		manifests[index] = testChainManifest(fmt.Sprintf("backup-%d", index), parent, incremental, uint64(index+1), 7, digit)
	}
	return manifests
}
