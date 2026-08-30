package hatSql

import "testing"

func TestMaterializedSingleSourceEnvelopeRetainsFieldsMetricsAndJoinData(t *testing.T) {
	t.Parallel()
	sourceRows := []Row{{"id": int64(1), "team": "core"}}
	rows := wrapSQLSource(sqlSource{alias: "event"}, sourceRows)
	if len(rows) != 1 || rows[0].singleAlias != "event" || rows[0].singleRow == nil {
		t.Fatalf("wrapSQLSource() = %#v", rows)
	}
	if value := sqlField(rows[0], "event", "team"); value != "core" {
		t.Fatalf("sqlField(single row) = %#v", value)
	}
	if got, want := sqlExecRowsBytes(rows), sqlRowBytes(sourceRows[0]); got != want {
		t.Fatalf("sqlExecRowsBytes(single row) = %d, want %d", got, want)
	}
	right := sqlExecRow{sources: map[string]Row{"detail": {"name": "worker"}}, order: []string{"detail"}, ordinals: map[string]int{"detail": 0}}
	merged := mergeSQLRows(rows[0], right)
	if value := sqlField(merged, "event", "id"); value != int64(1) {
		t.Fatalf("sqlField(merged left) = %#v", value)
	}
	if value := sqlField(merged, "detail", "name"); value != "worker" || merged.ordinals["event"] != 0 {
		t.Fatalf("mergeSQLRows() = %#v, event ordinal = %d", merged, merged.ordinals["event"])
	}
}
