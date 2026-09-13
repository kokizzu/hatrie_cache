package hatPipeline_test

import (
	"reflect"
	"testing"

	"hatrie_cache/hat/hatPipeline"
)

func TestMZ04FrontierRetentionSnapshotReportsCompactionDebt(t *testing.T) {
	frontiers, err := hatPipeline.NewFrontierRegistry(hatPipeline.FrontierRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer frontiers.Close()
	if err := frontiers.Register("events"); err != nil {
		t.Fatal(err)
	}
	if err := frontiers.Advance("events", 100, 100); err != nil {
		t.Fatal(err)
	}
	retention, err := hatPipeline.NewFrontierRetentionRegistry(frontiers, hatPipeline.FrontierRetentionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer retention.Close()
	lease, err := retention.Acquire("events", 100)
	if err != nil {
		t.Fatal(err)
	}
	secondLease, err := retention.Acquire("events", 100)
	if err != nil {
		t.Fatal(err)
	}
	if err := frontiers.Advance("events", 120, 130); err != nil {
		t.Fatal(err)
	}

	snapshot, err := retention.Snapshot("events")
	if err != nil {
		t.Fatal(err)
	}
	if got := mz04Uint64Field(t, snapshot, "CurrentLower"); got != 120 {
		t.Fatalf("frontier lower = %d, want 120", got)
	}
	if got := mz04Uint64Field(t, snapshot, "CurrentUpper"); got != 130 {
		t.Fatalf("frontier upper = %d, want 130", got)
	}
	if got := mz04Uint64Field(t, snapshot, "CompactionDebt"); got != 20 {
		t.Fatalf("compaction debt = %d, want 20", got)
	}
	if got := mz04BoolField(t, snapshot, "BlockedByLease"); !got {
		t.Fatalf("BlockedByLease = %v, want true", got)
	}
	if got := mz04IntField(t, snapshot, "BlockingLeaseCount"); got != 2 {
		t.Fatalf("BlockingLeaseCount = %d, want 2", got)
	}

	if err := retention.Release(lease); err != nil {
		t.Fatal(err)
	}
	snapshot, err = retention.Snapshot("events")
	if err != nil {
		t.Fatal(err)
	}
	if got := mz04IntField(t, snapshot, "BlockingLeaseCount"); got != 1 {
		t.Fatalf("one-release BlockingLeaseCount = %d, want 1", got)
	}
	if err := retention.Release(secondLease); err != nil {
		t.Fatal(err)
	}
	snapshot, err = retention.Snapshot("events")
	if err != nil {
		t.Fatal(err)
	}
	if got := mz04Uint64Field(t, snapshot, "CompactionDebt"); got != 0 {
		t.Fatalf("released compaction debt = %d, want 0", got)
	}
	if got := mz04BoolField(t, snapshot, "BlockedByLease"); got {
		t.Fatalf("released BlockedByLease = %v, want false", got)
	}
	if got := mz04IntField(t, snapshot, "BlockingLeaseCount"); got != 0 {
		t.Fatalf("released BlockingLeaseCount = %d, want 0", got)
	}
}

func mz04Uint64Field(t *testing.T, value any, name string) uint64 {
	t.Helper()
	field := reflect.ValueOf(value).FieldByName(name)
	if !field.IsValid() || field.Kind() != reflect.Uint64 {
		t.Fatalf("snapshot field %s is missing or has the wrong type", name)
	}
	return field.Uint()
}

func mz04BoolField(t *testing.T, value any, name string) bool {
	t.Helper()
	field := reflect.ValueOf(value).FieldByName(name)
	if !field.IsValid() || field.Kind() != reflect.Bool {
		t.Fatalf("snapshot field %s is missing or has the wrong type", name)
	}
	return field.Bool()
}

func mz04IntField(t *testing.T, value any, name string) int {
	t.Helper()
	field := reflect.ValueOf(value).FieldByName(name)
	if !field.IsValid() || field.Kind() != reflect.Int {
		t.Fatalf("snapshot field %s is missing or has the wrong type", name)
	}
	return int(field.Int())
}
