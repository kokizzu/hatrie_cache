package hatSql

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLProjectionAdvisorFilePersistenceRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "advisor.spa")
	store, err := NewFileSQLProjectionAdvisorStore(path)
	if err != nil {
		t.Fatal(err)
	}
	want := []SQLProjectionRecommendation{{
		QueryID:        "dashboard",
		Dependencies:   []string{"events"},
		Fields:         []string{"events.amount", "events.region"},
		FilterFields:   []string{"events.region"},
		GroupByFields:  []string{"events.region"},
		OrderByFields:  []string{"events.amount"},
		SlowQueries:    3,
		TotalElapsed:   15 * time.Millisecond,
		AverageElapsed: 5 * time.Millisecond,
	}}
	advisor := NewSQLProjectionAdvisor(4)
	if err := advisor.Restore(want); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	if err := advisor.Persist(context.Background(), store); err != nil {
		t.Fatalf("Persist() error = %v", err)
	}

	restored := NewSQLProjectionAdvisor(4)
	if err := restored.RestoreFrom(context.Background(), store); err != nil {
		t.Fatalf("RestoreFrom() error = %v", err)
	}
	got := restored.Recommendations()
	if len(got) != 1 || got[0].QueryID != want[0].QueryID || got[0].TotalElapsed != want[0].TotalElapsed || got[0].AverageElapsed != want[0].AverageElapsed {
		t.Fatalf("restored recommendations = %#v, want %#v", got, want)
	}
	if len(got[0].Fields) != 2 || got[0].Fields[0] != "events.amount" || got[0].Fields[1] != "events.region" {
		t.Fatalf("restored fields = %#v", got[0].Fields)
	}
}

func TestSQLProjectionAdvisorFilePersistenceRejectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "advisor.spa")
	store, err := NewFileSQLProjectionAdvisorStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSQLProjectionAdvisor(context.Background(), []SQLProjectionRecommendation{{
		QueryID: "q", Dependencies: []string{"events"}, SlowQueries: 1, TotalElapsed: time.Millisecond,
	}}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 1
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadSQLProjectionAdvisor(context.Background()); !errors.Is(err, ErrSQLProjectionAdvisorSnapshotCorrupt) {
		t.Fatalf("corrupt load error = %v, want ErrSQLProjectionAdvisorSnapshotCorrupt", err)
	}
}

func TestSQLProjectionAdvisorRestoreHonorsCapacityAndRejectsDuplicates(t *testing.T) {
	advisor := NewSQLProjectionAdvisor(1)
	entries := []SQLProjectionRecommendation{
		{QueryID: "a", Dependencies: []string{"events"}, SlowQueries: 1, TotalElapsed: time.Millisecond},
		{QueryID: "b", Dependencies: []string{"events"}, SlowQueries: 1, TotalElapsed: time.Millisecond},
	}
	if err := advisor.Restore(entries); !errors.Is(err, ErrSQLProjectionAdvisorSnapshotInvalid) {
		t.Fatalf("capacity restore error = %v, want ErrSQLProjectionAdvisorSnapshotInvalid", err)
	}
	if got := advisor.Recommendations(); len(got) != 0 {
		t.Fatalf("advisor after rejected capacity restore = %#v, want empty", got)
	}
	duplicate := []SQLProjectionRecommendation{
		{QueryID: "a", Dependencies: []string{"events"}, SlowQueries: 1, TotalElapsed: time.Millisecond},
		{QueryID: "a", Dependencies: []string{"events"}, SlowQueries: 2, TotalElapsed: 2 * time.Millisecond},
	}
	if err := NewSQLProjectionAdvisor(4).Restore(duplicate); !errors.Is(err, ErrSQLProjectionAdvisorSnapshotInvalid) {
		t.Fatalf("duplicate restore error = %v, want ErrSQLProjectionAdvisorSnapshotInvalid", err)
	}
}

func TestSQLProjectionAdvisorRestoreMissingFileIsEmpty(t *testing.T) {
	store, err := NewFileSQLProjectionAdvisorStore(filepath.Join(t.TempDir(), "missing.spa"))
	if err != nil {
		t.Fatal(err)
	}
	advisor := NewSQLProjectionAdvisor(2)
	if err := advisor.RestoreFrom(context.Background(), store); err != nil {
		t.Fatalf("RestoreFrom() missing file error = %v", err)
	}
	if got := advisor.Recommendations(); len(got) != 0 {
		t.Fatalf("missing-file recommendations = %#v, want empty", got)
	}
}

func TestSQLProjectionAdvisorRestoreFailureLeavesState(t *testing.T) {
	advisor := NewSQLProjectionAdvisor(2)
	initial := []SQLProjectionRecommendation{{
		QueryID:      "keep",
		Dependencies: []string{"events"},
		SlowQueries:  1,
		TotalElapsed: time.Millisecond,
	}}
	if err := advisor.Restore(initial); err != nil {
		t.Fatal(err)
	}
	invalid := []SQLProjectionRecommendation{
		{QueryID: "new", Dependencies: []string{"events"}, SlowQueries: 1, TotalElapsed: time.Millisecond},
		{QueryID: "new", Dependencies: []string{"events"}, SlowQueries: 2, TotalElapsed: 2 * time.Millisecond},
	}
	if err := advisor.Restore(invalid); !errors.Is(err, ErrSQLProjectionAdvisorSnapshotInvalid) {
		t.Fatalf("invalid restore error = %v, want ErrSQLProjectionAdvisorSnapshotInvalid", err)
	}
	got := advisor.Recommendations()
	if len(got) != 1 || got[0].QueryID != "keep" {
		t.Fatalf("advisor after failed restore = %#v, want initial state", got)
	}
}

func TestSQLProjectionAdvisorFilePersistenceRejectsBroadPermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "advisor.spa")
	store, err := NewFileSQLProjectionAdvisorStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSQLProjectionAdvisor(context.Background(), []SQLProjectionRecommendation{{
		QueryID: "q", Dependencies: []string{"events"}, SlowQueries: 1, TotalElapsed: time.Millisecond,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadSQLProjectionAdvisor(context.Background()); !errors.Is(err, ErrSQLProjectionAdvisorSnapshotInvalid) {
		t.Fatalf("broad-permission load error = %v, want ErrSQLProjectionAdvisorSnapshotInvalid", err)
	}
}

func TestSQLProjectionAdvisorSnapshotSize(t *testing.T) {
	frame, err := marshalSQLProjectionAdvisorSnapshot(benchmarkSQLProjectionAdvisorRecommendations())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("32-entry SPA1 snapshot bytes = %d", len(frame))
}
