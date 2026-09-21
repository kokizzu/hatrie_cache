package hatSql

import "testing"

func TestC225IncrementalWindowAggregatesPreservePartitionAndNullSemantics(t *testing.T) {
	rows := approximateAggregateSource{
		{"id": int64(1), "bucket": "a", "value": 1.0},
		{"id": int64(2), "bucket": "a", "value": 2.0},
		{"id": int64(3), "bucket": "a", "value": nil},
		{"id": int64(4), "bucket": "a", "value": 4.0},
		{"id": int64(5), "bucket": "b", "value": 10.0},
		{"id": int64(6), "bucket": "b", "value": 5.0},
	}
	result, err := ExecuteSQLQuery(`
		SELECT id,
			SUM(value) OVER (
				PARTITION BY bucket ORDER BY id
				ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
			) AS running_sum,
			AVG(value) OVER (
				PARTITION BY bucket ORDER BY id
				ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
			) AS running_avg,
			MIN(value) OVER (
				PARTITION BY bucket ORDER BY id
				ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
			) AS running_min,
			MAX(value) OVER (
				PARTITION BY bucket ORDER BY id
				ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
			) AS running_max
		FROM CACHE('events')
		ORDER BY id
	`, rows)
	if err != nil {
		t.Fatalf("execute incremental window query: %v", err)
	}
	want := []struct {
		sum, average, minimum, maximum interface{}
	}{
		{1.0, 1.0, 1.0, 1.0},
		{3.0, 1.5, 1.0, 2.0},
		{3.0, 1.5, 1.0, 2.0},
		{7.0, 7.0 / 3.0, 1.0, 4.0},
		{10.0, 10.0, 10.0, 10.0},
		{15.0, 7.5, 5.0, 10.0},
	}
	if len(result.Rows) != len(want) {
		t.Fatalf("result rows = %d, want %d", len(result.Rows), len(want))
	}
	for index, expected := range want {
		row := result.Rows[index]
		if row["id"] != int64(index+1) || row["running_sum"] != expected.sum || row["running_avg"] != expected.average || row["running_min"] != expected.minimum || row["running_max"] != expected.maximum {
			t.Fatalf("row %d = %#v, want id=%d sum=%v avg=%v min=%v max=%v", index, row, index+1, expected.sum, expected.average, expected.minimum, expected.maximum)
		}
	}
}
