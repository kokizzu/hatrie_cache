package hatSql

import (
	"testing"
	"time"
)

func TestTemporalWatermarkSessionsIntervalsAndRetention(t *testing.T) {
	table := NewTemporalTable()
	t0 := time.Unix(0, 0).UTC()
	t1 := t0.Add(time.Minute)
	table.Upsert("u1", t0, Row{"value": 1})
	table.Upsert("u1", t1, Row{"value": 2})
	if row, ok := table.AsOf("u1", t0.Add(30*time.Second)); !ok || row["value"] != 1 {
		t.Fatalf("AS OF = %#v", row)
	}
	watermark := NewWatermark(time.Minute)
	if watermark.Observe(t1) || !watermark.Observe(t0) {
		t.Fatal("watermark late handling")
	}
	sessions := Sessionize([]TimedRow{{At: t0, Row: Row{"id": 1}}, {At: t0.Add(30 * time.Second), Row: Row{"id": 2}}, {At: t0.Add(3 * time.Minute), Row: Row{"id": 3}}}, time.Minute)
	if len(sessions) != 2 {
		t.Fatalf("sessions=%#v", sessions)
	}
	if !IntervalsOverlap(TimeInterval{Start: t0, End: t1}, TimeInterval{Start: t0.Add(30 * time.Second), End: t1.Add(time.Minute)}) {
		t.Fatal("interval overlap")
	}
	if removed := table.RetainAfter(t1, true); removed != 1 {
		t.Fatalf("retained removals=%d", removed)
	}
}
