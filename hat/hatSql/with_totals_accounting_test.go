package hatSql

import "testing"

func TestSQLWithTotalsAccountingIncludesTotals(t *testing.T) {
	result := SQLQueryResult{
		Rows:   []SQLRow{{"region": "east", "total": int64(7)}},
		Totals: SQLRow{"region": nil, "total": int64(7)},
	}
	wantBytes := sqlRowsBytes(result.Rows) + sqlRowBytes(result.Totals)
	if got := sqlQueryResultOutputRows(result); got != 2 {
		t.Fatalf("output rows = %d, want 2", got)
	}
	if got := sqlQueryResultBytes(result); got != wantBytes {
		t.Fatalf("result bytes = %d, want %d", got, wantBytes)
	}
}
