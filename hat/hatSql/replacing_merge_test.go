package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func TestSQLReplacingMergeKeepsNewestVersionInStableKeyOrder(t *testing.T) {
	rows := []SQLRow{
		{"id": "a", "version": uint64(1), "value": "old"},
		{"id": "b", "version": uint64(4), "value": "b"},
		{"id": "a", "version": uint64(3), "value": "new"},
		{"id": "b", "version": uint64(2), "value": "stale"},
		{"id": "a", "version": uint64(3), "value": "tie-wins-later"},
	}

	merged, err := ReplaceSQLRows(rows,
		func(row SQLRow) string { return row["id"].(string) },
		func(row SQLRow) (uint64, error) { return row["version"].(uint64), nil },
	)
	if err != nil {
		t.Fatalf("ReplaceSQLRows() error = %v", err)
	}
	want := []SQLRow{
		{"id": "a", "version": uint64(3), "value": "tie-wins-later"},
		{"id": "b", "version": uint64(4), "value": "b"},
	}
	if !reflect.DeepEqual(merged, want) {
		t.Fatalf("merged = %#v, want %#v", merged, want)
	}
	if rows[0]["value"] != "old" || rows[4]["value"] != "tie-wins-later" {
		t.Fatal("ReplaceSQLRows() mutated input rows")
	}
	merged[0]["value"] = "changed-output"
	if rows[4]["value"] != "tie-wins-later" {
		t.Fatal("merged row aliases input row")
	}
}

func TestSQLReplacingMergeUsesLastRowWithoutVersion(t *testing.T) {
	rows := []SQLRow{
		{"id": "a", "value": 1},
		{"id": "a", "value": 2},
		{"id": "b", "value": 3},
	}
	merged, err := ReplaceSQLRows(rows, func(row SQLRow) string { return row["id"].(string) }, nil)
	if err != nil {
		t.Fatalf("ReplaceSQLRows() error = %v", err)
	}
	want := []SQLRow{{"id": "a", "value": 2}, {"id": "b", "value": 3}}
	if !reflect.DeepEqual(merged, want) {
		t.Fatalf("merged = %#v, want %#v", merged, want)
	}
}

func TestSQLReplacingMergeRejectsMissingOrFailingCallbacks(t *testing.T) {
	rows := []SQLRow{{"id": "a"}}
	if _, err := ReplaceSQLRows(rows, nil, nil); !errors.Is(err, ErrSQLReplacingMergeKeyRequired) {
		t.Fatalf("nil key error = %v, want ErrSQLReplacingMergeKeyRequired", err)
	}
	wantErr := errors.New("version failed")
	if _, err := ReplaceSQLRows(rows, func(SQLRow) string { return "a" }, func(SQLRow) (uint64, error) { return 0, wantErr }); !errors.Is(err, wantErr) {
		t.Fatalf("version error = %v, want %v", err, wantErr)
	}
}

func TestSQLReplacingMergeHandlesEmptyInput(t *testing.T) {
	merged, err := ReplaceSQLRows(nil, func(SQLRow) string { return "" }, nil)
	if err != nil {
		t.Fatalf("ReplaceSQLRows() error = %v", err)
	}
	if merged != nil {
		t.Fatalf("merged = %#v, want nil", merged)
	}
}

func BenchmarkSQLReplacingMerge(b *testing.B) {
	rows := make([]SQLRow, 1024)
	for index := range rows {
		rows[index] = SQLRow{
			"id":      index % 256,
			"version": uint64(index),
			"value":   index,
		}
	}
	key := func(row SQLRow) string { return string(rune(row["id"].(int))) }
	version := func(row SQLRow) (uint64, error) { return row["version"].(uint64), nil }
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := ReplaceSQLRows(rows, key, version); err != nil {
			b.Fatal(err)
		}
	}
}
