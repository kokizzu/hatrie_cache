package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func TestSQLCollapsingMergeCancelsOppositeSignsAndPreservesOrder(t *testing.T) {
	rows := []SQLRow{
		{"id": "a", "value": "first"},
		{"id": "a", "value": "second"},
		{"id": "b", "value": "delete"},
		{"id": "a", "value": "cancel-second"},
		{"id": "c", "value": "keep"},
		{"id": "b", "value": "restore"},
		{"id": "a", "value": "cancel-first"},
	}
	signs := []int{1, 1, -1, -1, 1, 1, -1}
	index := 0
	merged, err := CollapseSQLRows(rows,
		func(row SQLRow) string { return row["id"].(string) },
		func(SQLRow) (int, error) {
			sign := signs[index]
			index++
			return sign, nil
		},
	)
	if err != nil {
		t.Fatalf("CollapseSQLRows() error = %v", err)
	}
	want := []SQLRow{{"id": "c", "value": "keep"}}
	if !reflect.DeepEqual(merged, want) {
		t.Fatalf("merged = %#v, want %#v", merged, want)
	}
	merged[0]["value"] = "changed-output"
	if rows[4]["value"] != "keep" {
		t.Fatal("merged row aliases input row")
	}
}

func TestSQLCollapsingMergeKeepsUnmatchedRowsInInputOrder(t *testing.T) {
	rows := []SQLRow{
		{"id": "a", "value": "positive-1"},
		{"id": "b", "value": "negative"},
		{"id": "a", "value": "positive-2"},
	}
	merged, err := CollapseSQLRows(rows, func(row SQLRow) string { return row["id"].(string) }, func(row SQLRow) (int, error) {
		if row["id"] == "b" {
			return -1, nil
		}
		return 1, nil
	})
	if err != nil {
		t.Fatalf("CollapseSQLRows() error = %v", err)
	}
	want := []SQLRow{rows[0], rows[1], rows[2]}
	if !reflect.DeepEqual(merged, want) {
		t.Fatalf("merged = %#v, want %#v", merged, want)
	}
}

func TestSQLCollapsingMergeRejectsInvalidArguments(t *testing.T) {
	rows := []SQLRow{{"id": "a"}}
	if _, err := CollapseSQLRows(rows, nil, nil); !errors.Is(err, ErrSQLCollapsingMergeKeyRequired) {
		t.Fatalf("nil key error = %v, want ErrSQLCollapsingMergeKeyRequired", err)
	}
	if _, err := CollapseSQLRows(rows, func(SQLRow) string { return "a" }, nil); !errors.Is(err, ErrSQLCollapsingMergeSignRequired) {
		t.Fatalf("nil sign error = %v, want ErrSQLCollapsingMergeSignRequired", err)
	}
	if _, err := CollapseSQLRows(rows, func(SQLRow) string { return "a" }, func(SQLRow) (int, error) { return 0, nil }); !errors.Is(err, ErrSQLCollapsingMergeInvalidSign) {
		t.Fatalf("invalid sign error = %v, want ErrSQLCollapsingMergeInvalidSign", err)
	}
	wantErr := errors.New("sign failed")
	if _, err := CollapseSQLRows(rows, func(SQLRow) string { return "a" }, func(SQLRow) (int, error) { return 1, wantErr }); !errors.Is(err, wantErr) {
		t.Fatalf("sign callback error = %v, want %v", err, wantErr)
	}
}

func TestSQLCollapsingMergeHandlesEmptyInput(t *testing.T) {
	merged, err := CollapseSQLRows(nil, func(SQLRow) string { return "" }, func(SQLRow) (int, error) { return 1, nil })
	if err != nil {
		t.Fatalf("CollapseSQLRows() error = %v", err)
	}
	if merged != nil {
		t.Fatalf("merged = %#v, want nil", merged)
	}
}

func BenchmarkSQLCollapsingMerge(b *testing.B) {
	rows := make([]SQLRow, 1024)
	for index := range rows {
		rows[index] = SQLRow{
			"id":    index % 256,
			"value": index,
		}
	}
	key := func(row SQLRow) string { return string(rune(row["id"].(int))) }
	sign := func(row SQLRow) (int, error) {
		if row["value"].(int)%2 == 0 {
			return 1, nil
		}
		return -1, nil
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := CollapseSQLRows(rows, key, sign); err != nil {
			b.Fatal(err)
		}
	}
}
