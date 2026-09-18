package hatSql

import (
	"context"
	"testing"
	"time"
)

func TestCH008ColumnTTLMasksOnlyExpiredColumn(t *testing.T) {
	now := time.Unix(1700000000, 0)
	table, err := NewTypedTable(TypedTableSchema{
		Name: "column-ttl",
		Columns: []TypedTableColumn{
			{Name: "id", Kind: TypedTableInt64},
			{Name: "payload", Kind: TypedTableString, TTL: TypedTableTTLOptions{
				Mode: TypedTableTTLProcessingTime, Lifetime: time.Hour, Clock: func() time.Time { return now },
			}},
			{Name: "amount", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("row-1", []TypedTableValue{TypedInt64(1), TypedString("wide-value"), TypedInt64(42)}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	rows := table.Rows()
	if len(rows) != 1 {
		t.Fatalf("Rows length = %d, want 1", len(rows))
	}
	if rows[0]["payload"] != nil {
		t.Fatalf("expired payload = %#v, want NULL", rows[0]["payload"])
	}
	if rows[0]["amount"] != int64(42) {
		t.Fatalf("unexpired amount = %#v, want 42", rows[0]["amount"])
	}
	stats := table.Stats()
	if stats.RowCount != 1 {
		t.Fatalf("stats row count = %d, want 1", stats.RowCount)
	}
	if got := stats.Columns[1].NullCount; got != 1 {
		t.Fatalf("payload null count = %d, want 1", got)
	}
	if got := stats.Columns[1].ValueCount; got != 0 {
		t.Fatalf("payload value count = %d, want 0", got)
	}
	batch, found, err := table.ResolveSQLColumnarSource("CACHE", "column-ttl", []string{"payload", "amount"})
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("columnar source not found")
	}
	if got := batch.Columns["payload"][0]; got != nil {
		t.Fatalf("columnar expired payload = %#v, want NULL", got)
	}
	if got := batch.Columns["amount"][0]; got != int64(42) {
		t.Fatalf("columnar amount = %#v, want 42", got)
	}
}

func TestCH008ColumnTTLEventTimeAndHistogram(t *testing.T) {
	now := time.Unix(1700000000, 0)
	table, err := NewTypedTable(TypedTableSchema{
		Name: "column-ttl-event",
		Columns: []TypedTableColumn{
			{Name: "event_at", Kind: TypedTableInt64},
			{Name: "amount", Kind: TypedTableInt64, TTL: TypedTableTTLOptions{
				Mode: TypedTableTTLEventTime, Field: "event_at", Lifetime: time.Hour, Clock: func() time.Time { return now },
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("old", []TypedTableValue{TypedInt64(now.Add(-2 * time.Hour).UnixNano()), TypedInt64(10)}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("new", []TypedTableValue{TypedInt64(now.UnixNano()), TypedInt64(20)}); err != nil {
		t.Fatal(err)
	}
	rows := table.Rows()
	if len(rows) != 2 {
		t.Fatalf("Rows length = %d, want 2", len(rows))
	}
	if rows[0]["amount"] != nil || rows[1]["amount"] != int64(20) {
		t.Fatalf("event-time column TTL rows = %#v, want old NULL and new 20", rows)
	}
	histogram, err := table.Histogram("amount", TypedTableHistogramOptions{Bins: 2})
	if err != nil {
		t.Fatal(err)
	}
	if histogram.RowCount != 2 || histogram.NullCount != 1 || histogram.ValueCount != 1 {
		t.Fatalf("event-time histogram counts = row %d null %d value %d, want 2/1/1", histogram.RowCount, histogram.NullCount, histogram.ValueCount)
	}
	binCount := 0
	for _, bin := range histogram.Bins {
		binCount += bin.Count
	}
	if binCount != histogram.ValueCount {
		t.Fatalf("event-time histogram bin count = %d, want %d", binCount, histogram.ValueCount)
	}
}

func TestCH008ColumnTTLValidation(t *testing.T) {
	tests := []struct {
		name    string
		options TypedTableTTLOptions
		want    string
	}{
		{name: "processing field", options: TypedTableTTLOptions{Mode: TypedTableTTLProcessingTime, Field: "id", Lifetime: time.Hour}, want: "does not accept a field"},
		{name: "event missing field", options: TypedTableTTLOptions{Mode: TypedTableTTLEventTime, Lifetime: time.Hour}, want: "field is required"},
		{name: "event wrong kind", options: TypedTableTTLOptions{Mode: TypedTableTTLEventTime, Field: "name", Lifetime: time.Hour}, want: "must be an int64"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewTypedTable(TypedTableSchema{
				Name: "invalid-column-ttl",
				Columns: []TypedTableColumn{
					{Name: "id", Kind: TypedTableInt64, TTL: test.options},
					{Name: "name", Kind: TypedTableString},
				},
			})
			if err == nil || !ch008ContainsString(err.Error(), test.want) {
				t.Fatalf("NewTypedTable error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestCH008PurgeExpiredColumnsEmitsUpdateAndReleasesValue(t *testing.T) {
	now := time.Unix(1700000000, 0)
	table, err := NewTypedTable(TypedTableSchema{
		Name: "column-ttl-purge",
		Columns: []TypedTableColumn{
			{Name: "id", Kind: TypedTableInt64},
			{Name: "payload", Kind: TypedTableString, TTL: TypedTableTTLOptions{
				Mode: TypedTableTTLProcessingTime, Lifetime: time.Hour, Clock: func() time.Time { return now },
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("row-1", []TypedTableValue{TypedInt64(1), TypedString("wide-value")}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	changes, err := table.PurgeExpiredColumns(now)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || changes[0].Operation != "UPDATE" {
		t.Fatalf("purge changes = %#v, want one UPDATE", changes)
	}
	if changes[0].Before[1].String != "wide-value" || changes[0].After[1].Valid {
		t.Fatalf("purge change payload = before %#v after %#v", changes[0].Before[1], changes[0].After[1])
	}
	changes, err = table.PurgeExpiredColumns(now)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 0 {
		t.Fatalf("second purge changes = %#v, want none", changes)
	}
}

func TestCH008ColumnTTLDeadlinesFollowPatchCompaction(t *testing.T) {
	now := time.Unix(1700000000, 0)
	table, err := NewTypedTable(TypedTableSchema{
		Name: "column-ttl-compaction",
		Columns: []TypedTableColumn{
			{Name: "payload", Kind: TypedTableString, TTL: TypedTableTTLOptions{
				Mode: TypedTableTTLProcessingTime, Lifetime: time.Hour, Clock: func() time.Time { return now },
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("old", []TypedTableValue{TypedString("old")}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	if _, err := table.Upsert("keep", []TypedTableValue{TypedString("keep")}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Delete("old"); err != nil {
		t.Fatal(err)
	}
	if err := table.CompactPatchParts(); err != nil {
		t.Fatal(err)
	}
	now = now.Add(30 * time.Minute)
	rows := table.Rows()
	if len(rows) != 1 || rows[0]["payload"] != "keep" {
		t.Fatalf("rows after column TTL patch compaction = %#v, want keep", rows)
	}
}

func TestCH008ColumnTTLStateRoundTrip(t *testing.T) {
	now := time.Unix(1700000000, 0)
	newTable := func(clock *time.Time) *TypedTable {
		table, err := NewTypedTable(TypedTableSchema{
			Name: "column-ttl-state",
			Columns: []TypedTableColumn{
				{Name: "payload", Kind: TypedTableString, TTL: TypedTableTTLOptions{
					Mode: TypedTableTTLProcessingTime, Lifetime: time.Hour, Clock: func() time.Time { return *clock },
				}},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return table
	}
	table := newTable(&now)
	if _, err := table.Upsert("row", []TypedTableValue{TypedString("payload")}); err != nil {
		t.Fatal(err)
	}
	encoded, err := table.MarshalColumnTTLState()
	if err != nil {
		t.Fatal(err)
	}
	restoredClock := now.Add(2 * time.Hour)
	restored := newTable(&restoredClock)
	if _, err := restored.Upsert("row", []TypedTableValue{TypedString("payload")}); err != nil {
		t.Fatal(err)
	}
	if restored.Rows()[0]["payload"] != nil {
		t.Fatal("restored table unexpectedly expired before state restore")
	}
	if err := restored.RestoreColumnTTLState(encoded); err != nil {
		t.Fatal(err)
	}
	if restored.Rows()[0]["payload"] != nil {
		t.Fatal("restored table did not apply expired deadline")
	}
	corrupted := append([]byte(nil), encoded...)
	corrupted[len(corrupted)-1] ^= 1
	if err := restored.RestoreColumnTTLState(corrupted); err == nil {
		t.Fatal("RestoreColumnTTLState accepted corrupted state")
	}
}

func TestCH008ColumnTTLSchedulerPurgesPhysicalValues(t *testing.T) {
	now := time.Unix(1700000000, 0)
	table, err := NewTypedTable(TypedTableSchema{
		Name: "column-ttl-scheduler",
		Columns: []TypedTableColumn{{Name: "payload", Kind: TypedTableString, TTL: TypedTableTTLOptions{
			Mode: TypedTableTTLProcessingTime, Lifetime: time.Hour, Clock: func() time.Time { return now },
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("row", []TypedTableValue{TypedString("payload")}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	scheduler, err := NewTypedTableTTLScheduler(TypedTableTTLSchedulerOptions{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Register("column-ttl-scheduler", table); err != nil {
		t.Fatal(err)
	}
	runs, err := scheduler.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || runs[0].Expired != 0 || runs[0].ExpiredColumns != 1 {
		t.Fatalf("scheduler runs = %#v, want one column expiry", runs)
	}
	if table.columns[0].valid[0] {
		t.Fatal("scheduler did not physically clear expired column")
	}
}

func ch008ContainsString(value, want string) bool {
	for index := 0; index+len(want) <= len(value); index++ {
		if value[index:index+len(want)] == want {
			return true
		}
	}
	return false
}
