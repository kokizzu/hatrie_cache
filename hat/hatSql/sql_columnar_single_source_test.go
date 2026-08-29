package hatSql

import "testing"

func TestSQLSingleSourceExecRow(t *testing.T) {
	row := newSQLSingleSourceExecRow("event", SQLRow{"id": int64(7)})
	if row.sources != nil || row.order != nil || row.ordinals != nil {
		t.Fatalf("single-source row retained map or slice envelopes: %#v", row)
	}
	if got := sqlField(row, "event", "id"); got != int64(7) {
		t.Fatalf("qualified sqlField() = %#v, want 7", got)
	}
	if got := sqlField(row, "", "id"); got != int64(7) {
		t.Fatalf("unqualified sqlField() = %#v, want 7", got)
	}
}
