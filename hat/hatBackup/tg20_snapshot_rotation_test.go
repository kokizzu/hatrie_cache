package hatBackup

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestTG20SnapshotRotationPolicyCadenceAndDefaults(t *testing.T) {
	now := time.Date(2026, time.January, 10, 12, 0, 0, 0, time.UTC)
	disabled, err := NewSnapshotRotationPolicy(SnapshotRotationOptions{})
	if err != nil {
		t.Fatalf("disabled policy: %v", err)
	}
	if disabled.Options().Enabled {
		t.Fatal("zero policy is enabled")
	}
	if disabled.ShouldSnapshot(time.Time{}, now) {
		t.Fatal("disabled policy is due")
	}

	policy, err := NewSnapshotRotationPolicy(SnapshotRotationOptions{Enabled: true})
	if err != nil {
		t.Fatalf("enabled policy: %v", err)
	}
	if got := policy.Options().Cadence; got != DefaultSnapshotRotationCadence {
		t.Fatalf("default cadence = %s, want %s", got, DefaultSnapshotRotationCadence)
	}
	if got := policy.Options().KeepLatest; got != DefaultSnapshotRotationKeepLatest {
		t.Fatalf("default keep latest = %d, want %d", got, DefaultSnapshotRotationKeepLatest)
	}
	if !policy.ShouldSnapshot(time.Time{}, now) {
		t.Fatal("enabled policy is not due for its first snapshot")
	}
	last := now.Add(-DefaultSnapshotRotationCadence)
	if !policy.ShouldSnapshot(last, now) {
		t.Fatal("enabled policy is not due at cadence")
	}
	if policy.ShouldSnapshot(last.Add(time.Nanosecond*2), last.Add(time.Nanosecond*2+time.Nanosecond)) {
		t.Fatal("enabled policy became due before cadence")
	}
}

func TestTG20SnapshotRotationPlanKeepsCompleteChains(t *testing.T) {
	baseTime := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	manifests := []BundleManifest{
		tg20Manifest("base-a", "", baseTime, 10, 100),
		tg20Manifest("inc-a-1", "base-a", baseTime.Add(24*time.Hour), 20, 20),
		tg20Manifest("inc-a-2", "inc-a-1", baseTime.Add(48*time.Hour), 30, 20),
		tg20Manifest("base-b", "", baseTime.Add(72*time.Hour), 40, 200),
		tg20Manifest("inc-b-1", "base-b", baseTime.Add(96*time.Hour), 50, 10),
	}

	policy, err := NewSnapshotRotationPolicy(SnapshotRotationOptions{KeepLatest: 2})
	if err != nil {
		t.Fatalf("policy: %v", err)
	}
	plan, err := policy.Plan(manifests)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if got, want := plan.SelectedTips, []string{"inc-b-1", "inc-a-2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("selected tips = %#v, want %#v", got, want)
	}
	if got, want := plan.RetainedBackupIDs, []string{"base-a", "inc-a-1", "inc-a-2", "base-b", "inc-b-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("retained IDs = %#v, want %#v", got, want)
	}
	if len(plan.DeleteBackupIDs) != 0 || plan.OverBudget {
		t.Fatalf("unbounded plan = %+v, want no deletes and no budget violation", plan)
	}
	if plan.RetainedBytes != 350 {
		t.Fatalf("retained bytes = %d, want 350", plan.RetainedBytes)
	}

	bounded, err := NewSnapshotRotationPolicy(SnapshotRotationOptions{KeepLatest: 2, MaxBytes: 250})
	if err != nil {
		t.Fatalf("bounded policy: %v", err)
	}
	boundedPlan, err := bounded.Plan(manifests)
	if err != nil {
		t.Fatalf("bounded plan: %v", err)
	}
	if got, want := boundedPlan.RetainedBackupIDs, []string{"base-b", "inc-b-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("bounded retained IDs = %#v, want %#v", got, want)
	}
	if got, want := boundedPlan.DeleteBackupIDs, []string{"base-a", "inc-a-1", "inc-a-2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("bounded delete IDs = %#v, want %#v", got, want)
	}
	if boundedPlan.RetainedBytes != 210 || boundedPlan.OverBudget {
		t.Fatalf("bounded plan = %+v, want 210 retained bytes and no budget violation", boundedPlan)
	}

	tight, err := NewSnapshotRotationPolicy(SnapshotRotationOptions{KeepLatest: 1, MaxBytes: 100})
	if err != nil {
		t.Fatalf("tight policy: %v", err)
	}
	tightPlan, err := tight.Plan(manifests)
	if err != nil {
		t.Fatalf("tight plan: %v", err)
	}
	if !tightPlan.OverBudget || tightPlan.RetainedBytes != 210 {
		t.Fatalf("tight plan = %+v, want unavoidable over-budget latest chain", tightPlan)
	}
}

func TestTG20SnapshotRotationPolicyRejectsInvalidOptions(t *testing.T) {
	for name, options := range map[string]SnapshotRotationOptions{
		"negative cadence": {Cadence: -time.Second},
		"negative keep":    {KeepLatest: -1},
		"negative bytes":   {MaxBytes: -1},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewSnapshotRotationPolicy(options); err == nil {
				t.Fatal("NewSnapshotRotationPolicy() error = nil")
			}
		})
	}
}

func TestTG20SnapshotRotationCatalogPlan(t *testing.T) {
	manifests := []BundleManifest{
		tg20Manifest("base", "", time.Unix(1, 0), 1, 100),
		tg20Manifest("child", "base", time.Unix(2, 0), 2, 20),
	}
	catalog, err := NewBackupManifestCatalog(filepath.Join(t.TempDir(), "manifests.log"))
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Replace(manifests); err != nil {
		t.Fatalf("catalog replace: %v", err)
	}
	policy, err := NewSnapshotRotationPolicy(SnapshotRotationOptions{KeepLatest: 1})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := catalog.PlanRotation(policy)
	if err != nil {
		t.Fatalf("catalog plan rotation: %v", err)
	}
	if !reflect.DeepEqual(plan.SelectedTips, []string{"child"}) {
		t.Fatalf("selected tips = %#v, want child", plan.SelectedTips)
	}
}

func TestTG20SnapshotRotationPlanRejectsByteOverflow(t *testing.T) {
	maxInt64 := int64(^uint64(0) >> 1)
	manifests := []BundleManifest{
		tg20Manifest("base", "", time.Unix(1, 0), 1, maxInt64),
		tg20Manifest("child", "base", time.Unix(2, 0), 2, 1),
	}
	policy, err := NewSnapshotRotationPolicy(SnapshotRotationOptions{KeepLatest: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := policy.Plan(manifests); err == nil {
		t.Fatal("overflow plan error = nil")
	}
}

func BenchmarkTG20SnapshotRotationPlan(b *testing.B) {
	manifests := tg20BenchmarkManifests()
	policy, err := NewSnapshotRotationPolicy(SnapshotRotationOptions{KeepLatest: 2, MaxBytes: 128 << 10})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		plan, err := policy.Plan(manifests)
		if err != nil {
			b.Fatal(err)
		}
		if len(plan.RetainedBackupIDs) == 0 {
			b.Fatal("rotation plan retained no backups")
		}
	}
}

func BenchmarkTG20BaselineBackupChainPlan(b *testing.B) {
	manifests := tg20BenchmarkManifests()
	latestID := manifests[len(manifests)-1].BackupID
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		plan, err := PlanBackupChain(manifests, latestID)
		if err != nil {
			b.Fatal(err)
		}
		if plan.LatestBackupID != latestID {
			b.Fatal("baseline chain plan selected the wrong tip")
		}
	}
}

func BenchmarkTG20SnapshotRotationDue(b *testing.B) {
	policy, err := NewSnapshotRotationPolicy(SnapshotRotationOptions{Enabled: true, Cadence: time.Hour})
	if err != nil {
		b.Fatal(err)
	}
	last := time.Unix(100, 0)
	now := last.Add(time.Hour)
	b.ReportAllocs()
	var due bool
	for index := 0; index < b.N; index++ {
		due = policy.ShouldSnapshot(last, now)
	}
	if !due {
		b.Fatal("snapshot should be due")
	}
}

func tg20Manifest(id, parent string, created time.Time, sequence uint64, bytes int64) BundleManifest {
	digest := sha256.Sum256([]byte(id))
	return BundleManifest{
		Version:           BundleVersion,
		CreatedAt:         created,
		Mode:              ModePebbleIncremental,
		Snapshot:          id,
		BackupID:          id,
		ParentBackupID:    parent,
		Incremental:       parent != "",
		Store:             "cache",
		NewObjectBytes:    bytes,
		StorageBackend:    "pebble",
		StorageFormat:     "v1",
		StorageIdentity:   "cache",
		StorageGeneration: 1,
		JournalSequence:   sequence,
		Files:             []BundleFile{{Path: id + ".snapshot", Size: bytes, SHA256: hex.EncodeToString(digest[:])}},
	}
}

func benchmarkTG20Integer(value int) string {
	if value == 0 {
		return "0"
	}
	var digits [20]byte
	index := len(digits)
	for value > 0 {
		index--
		digits[index] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[index:])
}

func tg20BenchmarkManifests() []BundleManifest {
	baseTime := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	manifests := make([]BundleManifest, 0, 64)
	parent := ""
	for index := 0; index < 64; index++ {
		id := "backup-" + benchmarkTG20Integer(index)
		manifests = append(manifests, tg20Manifest(id, parent, baseTime.Add(time.Duration(index)*time.Hour), uint64(index+1), 1024))
		parent = id
	}
	return manifests
}
