package hatSql

import "testing"

func TestSQLColumnarSourceExecRow(t *testing.T) {
	batch := ColumnarBatch{Columns: map[string][]interface{}{"id": {int64(7)}}, Rows: 1}
	row := newSQLColumnarSourceExecRow("event", &batch, 0)
	if row.sources != nil || row.order != nil || row.ordinals != nil || row.singleRow != nil {
		t.Fatalf("columnar row retained map or slice envelopes: %#v", row)
	}
	if got := sqlField(row, "event", "id"); got != int64(7) {
		t.Fatalf("qualified sqlField() = %#v, want 7", got)
	}
	if got := sqlField(row, "", "id"); got != int64(7) {
		t.Fatalf("unqualified sqlField() = %#v, want 7", got)
	}
}
