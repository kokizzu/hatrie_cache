package hatSql

import (
	"testing"
	"time"
)

func TestC239PartMergeMetricsReportBacklogAgeAndAmplification(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableInt64}},
		PatchParts: TypedTablePatchOptions{
			Enabled:        true,
			MergeThreshold: 100,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.September, 23, 12, 0, 0, 0, time.UTC)
	table.patchParts.now = func() time.Time { return now }
	for index := 0; index < 4; index++ {
		if _, err := table.Upsert(string(rune('a'+index)), []TypedTableValue{TypedInt64(int64(index))}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := table.Delete("a"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(45 * time.Second)
	metrics := table.PartMergeMetrics()
	if metrics.PendingDeletes != 1 {
		t.Fatalf("pending deletes = %d, want 1", metrics.PendingDeletes)
	}
	if metrics.OldestPendingAge != 45*time.Second {
		t.Fatalf("oldest pending age = %s, want 45s", metrics.OldestPendingAge)
	}
	if metrics.MergeCount != 0 || metrics.RowsRead != 0 || metrics.RowsWritten != 0 || metrics.DeletedRows != 0 {
		t.Fatalf("pre-merge metrics = %#v", metrics)
	}

	if err := table.CompactPatchParts(); err != nil {
		t.Fatal(err)
	}
	metrics = table.PartMergeMetrics()
	if metrics.PendingDeletes != 0 || metrics.OldestPendingAge != 0 {
		t.Fatalf("post-merge backlog metrics = %#v", metrics)
	}
	if metrics.MergeCount != 1 || metrics.RowsRead != 4 || metrics.RowsWritten != 3 || metrics.DeletedRows != 1 {
		t.Fatalf("post-merge counters = %#v", metrics)
	}
	if metrics.WriteAmplification != 3 {
		t.Fatalf("write amplification = %v, want 3", metrics.WriteAmplification)
	}
	if metrics.LastMergeAt != now {
		t.Fatalf("last merge at = %s, want %s", metrics.LastMergeAt, now)
	}

	now = now.Add(10 * time.Second)
	if _, err := table.Delete("b"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(10 * time.Second)
	metrics = table.PartMergeMetrics()
	if metrics.PendingDeletes != 1 || metrics.OldestPendingAge != 10*time.Second {
		t.Fatalf("second backlog metrics = %#v", metrics)
	}
}

func TestC239PartMergeMetricsDisabledByDefault(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableInt64}},
	})
	if err != nil {
		t.Fatal(err)
	}
	metrics := table.PartMergeMetrics()
	if metrics != (TypedTablePartMergeMetrics{}) {
		t.Fatalf("default metrics = %#v, want zero", metrics)
	}
}
