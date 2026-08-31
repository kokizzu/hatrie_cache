package hatSql

import "testing"

func TestSQLColumnarStreamMaterializeStopsAfterLimit(t *testing.T) {
	query := &sqlQuery{
		limit: 2,
		selects: []sqlSelectItem{{
			expr: sqlExpr{kind: "field", name: "id"},
		}},
	}
	batch := ColumnarBatch{
		Columns: map[string][]interface{}{"id": {int64(1), int64(2), int64(3), int64(4), int64(5)}},
		Rows:    5,
	}
	visited := 0
	result, matched := sqlColumnarStreamMaterialize(query, batch, []string{"id"}, func(int) bool {
		visited++
		return true
	})
	if visited != 2 || matched != 2 {
		t.Fatalf("visited/matched = %d/%d, want 2/2", visited, matched)
	}
	if len(result.Rows) != 2 || result.Rows[0]["id"] != int64(1) || result.Rows[1]["id"] != int64(2) {
		t.Fatalf("rows = %#v, want first two rows", result.Rows)
	}
}

func TestSQLColumnarStreamMaterializeWithScanRetainsAllMatches(t *testing.T) {
	query := &sqlQuery{
		limit: 2,
		selects: []sqlSelectItem{{
			expr: sqlExpr{kind: "field", name: "id"},
		}},
	}
	batch := ColumnarBatch{
		Columns: map[string][]interface{}{"id": {int64(1), int64(2), int64(3), int64(4), int64(5)}},
		Rows:    5,
	}
	visited := 0
	result, matched := sqlColumnarStreamMaterializeWithScan(query, batch, []string{"id"}, func(int) bool {
		visited++
		return true
	}, true)
	if visited != 5 || matched != 5 {
		t.Fatalf("visited/matched = %d/%d, want 5/5", visited, matched)
	}
	if len(result.Rows) != 2 || result.Rows[0]["id"] != int64(1) || result.Rows[1]["id"] != int64(2) {
		t.Fatalf("rows = %#v, want first two rows", result.Rows)
	}
}
