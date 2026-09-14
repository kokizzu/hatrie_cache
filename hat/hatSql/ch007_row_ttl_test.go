package hatSql

import (
	"reflect"
	"testing"
	"time"
)

type ch007ManualClock struct {
	now time.Time
}

func (clock *ch007ManualClock) Now() time.Time {
	return clock.now
}

func TestCH007ProcessingTimeTTLFiltersRowsAndEmitsDeletes(t *testing.T) {
	clock := &ch007ManualClock{now: time.Unix(100, 0)}
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableInt64}},
		TTL: TypedTableTTLOptions{
			Mode:     TypedTableTTLProcessingTime,
			Lifetime: 10 * time.Second,
			Clock:    clock.Now,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("old", []TypedTableValue{TypedInt64(1)}); err != nil {
		t.Fatal(err)
	}
	clock.now = time.Unix(105, 0)
	if _, err := table.Upsert("new", []TypedTableValue{TypedInt64(2)}); err != nil {
		t.Fatal(err)
	}
	clock.now = time.Unix(110, 0)
	if rows := table.Rows(); !reflect.DeepEqual(rows, []Row{{"value": int64(2)}}) {
		t.Fatalf("Rows() = %#v, want only the live processing-time row", rows)
	}
	if count, available, exact, err := table.SQLSourceCardinality("CACHE", "events"); err != nil || !available || !exact || count != 1 {
		t.Fatalf("SQLSourceCardinality() = %d/%v/%v/%v, want 1/true/true/nil", count, available, exact, err)
	}
	changes, err := table.PurgeExpired(clock.now)
	if err != nil {
		t.Fatal(err)
	}
	wantChanges := []TypedTableChange{{Operation: "DELETE", Key: "old", Before: []TypedTableValue{TypedInt64(1)}}}
	if len(changes) != 1 || changes[0].Operation != wantChanges[0].Operation || changes[0].Key != wantChanges[0].Key || !reflect.DeepEqual(changes[0].Before, wantChanges[0].Before) {
		t.Fatalf("PurgeExpired() = %#v, want delete for old", changes)
	}
	if rows := table.Rows(); !reflect.DeepEqual(rows, []Row{{"value": int64(2)}}) {
		t.Fatalf("Rows() after purge = %#v, want new row", rows)
	}
}

func TestCH007EventTimeTTLUsesFieldAndLeavesNullRows(t *testing.T) {
	eventAt := func(seconds int64) int64 { return time.Unix(seconds, 0).UnixNano() }
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "event_at", Kind: TypedTableInt64},
			{Name: "value", Kind: TypedTableString},
		},
		TTL: TypedTableTTLOptions{
			Mode:     TypedTableTTLEventTime,
			Field:    "event_at",
			Lifetime: 10 * time.Second,
			Clock:    func() time.Time { return time.Unix(110, 0) },
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for key, values := range map[string][]TypedTableValue{
		"expired": {TypedInt64(eventAt(100)), TypedString("expired")},
		"live":    {TypedInt64(eventAt(105)), TypedString("live")},
		"unknown": {TypedNull(), TypedString("unknown")},
	} {
		if _, err := table.Upsert(key, values); err != nil {
			t.Fatal(err)
		}
	}
	changes, err := table.PurgeExpired(time.Unix(110, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || changes[0].Key != "expired" {
		t.Fatalf("event-time purge changes = %#v, want expired only", changes)
	}
	rows := table.Rows()
	if len(rows) != 2 {
		t.Fatalf("event-time rows = %#v, want live and unknown", rows)
	}
}

func TestCH007TTLRenewsOnUpdateAndValidatesSchema(t *testing.T) {
	clock := &ch007ManualClock{now: time.Unix(100, 0)}
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableInt64}},
		TTL:     TypedTableTTLOptions{Mode: TypedTableTTLProcessingTime, Lifetime: time.Second, Clock: clock.Now},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("key", []TypedTableValue{TypedInt64(1)}); err != nil {
		t.Fatal(err)
	}
	clock.now = time.Unix(100, int64(500*time.Millisecond))
	if _, err := table.Upsert("key", []TypedTableValue{TypedInt64(2)}); err != nil {
		t.Fatal(err)
	}
	if changes, err := table.PurgeExpired(time.Unix(101, int64(400*time.Millisecond))); err != nil || len(changes) != 0 {
		t.Fatalf("renewal purge = %#v/%v, want no delete", changes, err)
	}
	if _, err := NewTypedTable(TypedTableSchema{
		Name:    "bad",
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableInt64}},
		TTL:     TypedTableTTLOptions{Mode: TypedTableTTLEventTime, Field: "missing", Lifetime: time.Second},
	}); err == nil {
		t.Fatal("event-time TTL accepted a missing field")
	}
	if _, err := NewTypedTable(TypedTableSchema{
		Name:    "bad-kind",
		Columns: []TypedTableColumn{{Name: "event_at", Kind: TypedTableString}},
		TTL:     TypedTableTTLOptions{Mode: TypedTableTTLEventTime, Field: "event_at", Lifetime: time.Second},
	}); err == nil {
		t.Fatal("event-time TTL accepted a non-integer field")
	}
}

func TestCH007DefaultTTLDoesNotChangeRows(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableInt64}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("key", []TypedTableValue{TypedInt64(1)}); err != nil {
		t.Fatal(err)
	}
	if changes, err := table.PurgeExpired(time.Unix(1, 0)); err != nil || len(changes) != 0 {
		t.Fatalf("default purge = %#v/%v, want no-op", changes, err)
	}
	if rows := table.Rows(); !reflect.DeepEqual(rows, []Row{{"value": int64(1)}}) {
		t.Fatalf("default rows = %#v, want original row", rows)
	}
}

func TestCH007TTLMatchesColumnarAndPatchVisibility(t *testing.T) {
	clock := &ch007ManualClock{now: time.Unix(100, 0)}
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableInt64}},
		TTL: TypedTableTTLOptions{
			Mode:     TypedTableTTLProcessingTime,
			Lifetime: 10 * time.Second,
			Clock:    clock.Now,
		},
		ColumnarCache: TypedTableColumnarCacheOptions{Enabled: true, MinReads: 1},
		PatchParts:    TypedTablePatchOptions{Enabled: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("old", []TypedTableValue{TypedInt64(1)}); err != nil {
		t.Fatal(err)
	}
	clock.now = time.Unix(105, 0)
	if _, err := table.Upsert("new", []TypedTableValue{TypedInt64(2)}); err != nil {
		t.Fatal(err)
	}
	clock.now = time.Unix(110, 0)
	batch, available, err := table.ResolveSQLColumnarSource("CACHE", "events", []string{"value"})
	if err != nil || !available || batch.Rows != 1 || !reflect.DeepEqual(batch.Columns["value"], []interface{}{int64(2)}) {
		t.Fatalf("TTL columnar source = %#v/%v/%v, want one live row", batch, available, err)
	}
	if changes, err := table.PurgeExpired(clock.now); err != nil || len(changes) != 1 || changes[0].Key != "old" {
		t.Fatalf("TTL patch purge = %#v/%v, want old delete", changes, err)
	}
	clock.now = time.Unix(111, 0)
	if _, err := table.Upsert("old", []TypedTableValue{TypedInt64(3)}); err != nil {
		t.Fatal(err)
	}
	if count, _, _, err := table.SQLSourceCardinality("CACHE", "events"); err != nil || count != 2 {
		t.Fatalf("TTL patch reinsert cardinality = %d/%v, want 2", count, err)
	}
}

func TestCH007ProcessingTTLTracksRowsThroughPatchCompaction(t *testing.T) {
	now := time.Unix(0, 0)
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		TTL: TypedTableTTLOptions{
			Mode:     TypedTableTTLProcessingTime,
			Lifetime: 10 * time.Second,
			Clock:    func() time.Time { return now },
		},
		PatchParts: TypedTablePatchOptions{
			Enabled:        true,
			MergeThreshold: 100,
		},
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableInt64}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("old", []TypedTableValue{TypedInt64(1)}); err != nil {
		t.Fatal(err)
	}
	now = time.Unix(5, 0)
	if _, err := table.Upsert("live", []TypedTableValue{TypedInt64(2)}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Delete("old"); err != nil {
		t.Fatal(err)
	}
	if err := table.CompactPatchParts(); err != nil {
		t.Fatal(err)
	}

	now = time.Unix(12, 0)
	rows := table.Rows()
	if len(rows) != 1 || rows[0]["value"] != int64(2) {
		t.Fatalf("rows after patch compaction = %#v, want live row retained", rows)
	}
}

func TestCH007TTLStatsAndHistogramFollowClock(t *testing.T) {
	now := time.Unix(0, 0)
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		TTL: TypedTableTTLOptions{
			Mode:     TypedTableTTLProcessingTime,
			Lifetime: 10 * time.Second,
			Clock:    func() time.Time { return now },
		},
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableInt64}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("old", []TypedTableValue{TypedInt64(1)}); err != nil {
		t.Fatal(err)
	}
	now = time.Unix(5, 0)
	if _, err := table.Upsert("live", []TypedTableValue{TypedInt64(2)}); err != nil {
		t.Fatal(err)
	}

	if stats := table.Stats(); stats.RowCount != 2 {
		t.Fatalf("initial stats = %#v, want two rows", stats)
	}
	histogram, err := table.Histogram("value", TypedTableHistogramOptions{Bins: 2})
	if err != nil {
		t.Fatal(err)
	}
	if histogram.RowCount != 2 {
		t.Fatalf("initial histogram = %#v, want two rows", histogram)
	}

	now = time.Unix(11, 0)
	stats := table.Stats()
	if stats.RowCount != 1 || stats.Columns[0].Min.Int64 != 2 || stats.Columns[0].Max.Int64 != 2 {
		t.Fatalf("expired-row stats = %#v, want only live row", stats)
	}
	histogram, err = table.Histogram("value", TypedTableHistogramOptions{Bins: 2})
	if err != nil {
		t.Fatal(err)
	}
	if histogram.RowCount != 1 || histogram.Min.Int64 != 2 || histogram.Max.Int64 != 2 {
		t.Fatalf("expired-row histogram = %#v, want only live row", histogram)
	}
}

func TestCH007EventTTLDoesNotReadClockOnUpsert(t *testing.T) {
	now := time.Unix(100, 0)
	clockCalls := 0
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		TTL: TypedTableTTLOptions{
			Mode:     TypedTableTTLEventTime,
			Field:    "event_time",
			Lifetime: time.Hour,
			Clock: func() time.Time {
				clockCalls++
				return now
			},
		},
		Columns: []TypedTableColumn{
			{Name: "event_time", Kind: TypedTableInt64},
			{Name: "value", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("event", []TypedTableValue{
		TypedInt64(now.UnixNano()),
		TypedInt64(1),
	}); err != nil {
		t.Fatal(err)
	}
	if clockCalls != 0 {
		t.Fatalf("event-time clock calls during Upsert = %d, want zero", clockCalls)
	}
	if rows := table.Rows(); len(rows) != 1 {
		t.Fatalf("event-time rows = %#v, want one row", rows)
	}
	if clockCalls != 1 {
		t.Fatalf("event-time clock calls during Rows = %d, want one", clockCalls)
	}
}
